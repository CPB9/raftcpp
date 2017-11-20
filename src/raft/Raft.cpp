/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <algorithm>

/* for varags */
#include <stdarg.h>

#include "Raft.h"
#include "Log.h"
#include "Node.h"

namespace raft
{

void Server::__log(NodeId node, const char *fmt, ...) const
{
    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    _saver->log(node, buf);
}

Server::Server(NodeId id, bool is_voting, ISender* sender, ISaver* saver)
    : _nodes(id, is_voting), _log(saver), _sender(sender), _saver(saver)
{
    _me.current_term = TermId(0);
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    _me.request_timeout = std::chrono::milliseconds(200);
    _me.election_timeout = std::chrono::milliseconds(1000);
    set_state(State::Follower);
}

void Server::become_leader()
{
    __log(_nodes.get_my_id(), "becoming leader term:%d", get_current_term());

    set_state(State::Leader);
    for (const Node& i: _nodes.items())
    {
        if (_nodes.is_me(i.get_id()))
            continue;
        _nodes.get_node(i.get_id()).unwrap().set_next_idx(_log.get_current_idx() + 1);
        _nodes.get_node(i.get_id()).unwrap().set_match_idx(0);
        send_appendentries(i);
    }
}

void Server::become_candidate()
{
    __log(_nodes.get_my_id(), "becoming candidate");

    set_current_term(get_current_term() + 1);
    _nodes.reset_all_votes();
    vote_for_nodeid(_nodes.get_my_id());
    _me.current_leader.clear();
    set_state(State::Candidate);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    _me.timeout_elapsed = std::chrono::milliseconds(_me.election_timeout.count() - 2 * (rand() % _me.election_timeout.count()));
    _sender->request_vote(MsgVoteReq(_me.current_term, _log.get_current_idx(), _log.get_last_log_term().unwrapOr(TermId(0))));
}

void Server::become_follower()
{
    __log(_nodes.get_my_id(), "becoming follower");
    set_state(State::Follower);
}

bmcl::Option<Error> Server::raft_periodic(std::chrono::milliseconds msec_since_last_period)
{
    _me.timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    if (1 == _nodes.get_num_voting_nodes())
    {
        const Node& node = _nodes.get_my_node();
        if (node.is_voting() && !is_leader())
            become_leader();
    }

    if (is_leader())
    {
        if (_me.request_timeout <= _me.timeout_elapsed)
        {
            send_appendentries_all();
            _me.timeout_elapsed = std::chrono::milliseconds(0);
        }
    }
    else if (_me.election_timeout <= _me.timeout_elapsed)
    {
        if (1 < _nodes.get_num_voting_nodes())
        {
            const Node& node = _nodes.get_my_node();
            if (node.is_voting())
            {
                __log(_nodes.get_my_id(), "election starting: %d %d, term: %d ci: %d",
                    _me.election_timeout.count(), _me.timeout_elapsed.count(), _me.current_term,
                    _log.get_current_idx());

                become_candidate();
            }
        }
    }

    auto r = _log.entry_apply_one();
    if (r.isOk())
    {
        const LogEntry& ety = r.unwrap();
        if (LotType::AddNode == ety.type)
        {
            assert(ety.node.isSome());
            NodeId id = ety.node.unwrap();
            entry_apply_node_add(ety, id);
        }
        __log(_nodes.get_my_id(), "applied log: %d, id: %d size: %d", _log.get_last_applied_idx(), ety.id, ety.data.data.size());
        return bmcl::None;
    }

    if (r.unwrapErr() == Error::NothingToApply)
        return bmcl::None;
    return r.unwrapErr();
}

bmcl::Option<Error> Server::accept_rep(NodeId nodeid, const MsgAppendEntriesRep& r)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    __log(nodeid,
          "received appendentries response %s ci:%d rci:%d 1stidx:%d",
          r.success ? "SUCCESS" : "fail",
        _log.get_current_idx(),
          r.current_idx,
          r.first_idx);

    if (node.isNone())
        return Error::NodeUnknown;

    /* Stale response -- ignore */
    if (r.current_idx != 0 && r.current_idx <= node->get_match_idx())
        return bmcl::None;

    if (!is_leader())
        return Error::NotLeader;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (_me.current_term < r.term)
    {
        set_current_term(r.term);
        become_follower();
        return bmcl::None;
    }

    if (_me.current_term > r.term)
        return bmcl::None;

    if (!r.success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        Index next_idx = node->get_next_idx();
        assert(0 < next_idx);
        if (r.current_idx < next_idx - 1)
            node->set_next_idx(std::min(r.current_idx + 1, _log.get_current_idx()));
        else
            node->set_next_idx(next_idx - 1);

        /* retry */
        send_appendentries(node.unwrap());
        return bmcl::None;
    }

    assert(r.current_idx <= _log.get_current_idx());

    node->set_next_idx(r.current_idx + 1);
    node->set_match_idx(r.current_idx);

    if (!node->is_voting() &&
        !_log.voting_change_is_in_progress() &&
        _log.get_current_idx() <= r.current_idx + 1 &&
        false == node->has_sufficient_logs()
        )
    {
        LogEntry ety(get_current_term(), EntryId(0), LotType::AddNode, node->get_id()); //insert node->get_id() into ety
        auto e = entry_append(ety);
        if (e.isSome())
            return e;

        node->set_has_sufficient_logs();
    }

    /* Update commit idx */
    Index point = r.current_idx;
    if (point > 0)
    {
        bmcl::Option<const LogEntry&> ety = _log.get_at_idx(point);
        assert(ety.isSome());
        if (!_log.is_committed(point) && ety.unwrap().term == _me.current_term && _nodes.is_committed(point))
        {
            _log.set_commit_idx(point);
        }
    }

    /* Aggressively send remaining entries */
    if (_log.get_at_idx(node->get_next_idx()).isSome())
        send_appendentries(node.unwrap());

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

bmcl::Result<MsgAppendEntriesRep, Error> Server::accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae)
{
    MsgAppendEntriesRep r(_me.current_term, false, 0, 0);
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    if (node.isNone())
        return Error::NodeUnknown;

    _me.timeout_elapsed = std::chrono::milliseconds(0);

    if (0 < ae.n_entries)
        __log(nodeid, "recvd appendentries t:%d ci:%d lc:%d pli:%d plt:%d #%d",
              ae.term,
              _log.get_current_idx(),
              ae.leader_commit,
              ae.prev_log_idx,
              ae.prev_log_term,
              ae.n_entries);

    r.term = _me.current_term;

    if (is_candidate() && _me.current_term == ae.term)
    {
        become_follower();
    }
    else if (ae.term > _me.current_term)
    {
        set_current_term(ae.term);
        r.term = ae.term;
        become_follower();
    }
    else if (ae.term < _me.current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        __log(nodeid, "AE term %d is less than current term %d", ae.term, _me.current_term);
        return MsgAppendEntriesRep(r.term, false, _log.get_current_idx(), 0);
    }

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae.prev_log_idx)
    {
        bmcl::Option<const LogEntry&> e = _log.get_at_idx(ae.prev_log_idx);
        if (e.isNone() || _log.get_current_idx() < ae.prev_log_idx)
        {
            /* 2. Reply false if log doesn't contain an entry at prevLogIndex
            whose term matches prevLogTerm (§5.3) */
            __log(nodeid, "AE no log at prev_idx %d", ae.prev_log_idx);
            return MsgAppendEntriesRep(r.term, false, _log.get_current_idx(), 0);
        }
    }

    r.current_idx = ae.prev_log_idx;

    Index i;
    for (i = 0; i < ae.n_entries; i++)
    {
        const LogEntry* ety = &ae.entries[i];
        Index ety_index = ae.prev_log_idx + 1 + i;
        r.current_idx = ety_index;
        bmcl::Option<const LogEntry&> existing_ety = _log.get_at_idx(ety_index);
        if (existing_ety.isNone())
            break;
        if (existing_ety.unwrap().term != ety->term && !_log.is_committed(ety_index))
        {
            /* 3. If an existing entry conflicts with a new one (same index
            but different terms), delete the existing entry and all that
            follow it (§5.3) */
            bool flag = true;
            while (_log.get_current_idx() >= ety_index)
            {
                bmcl::Option<LogEntry> pop = _log.entry_pop_back();
                if (pop.isNone())
                    flag = false;
                else
                    pop_log(pop.unwrap(), _log.get_current_idx() + 1);
            }
            break;
        }
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < ae.n_entries; i++)
    {
        bmcl::Option<Error> e = entry_append(ae.entries[i]);
        if (e.isSome())
        {
            if (e.unwrap() == Error::Shutdown)
            {
                r.success = false;
                r.first_idx = 0;
                return Error::Shutdown;
            }
            return MsgAppendEntriesRep(r.term, false, _log.get_current_idx(), 0);
        }
        r.current_idx = ae.prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    _log.commit_till(ae.leader_commit);

    /* update current leader because we accepted appendentries from it */
    _me.current_leader = nodeid;

    r.success = true;
    r.first_idx = ae.prev_log_idx + 1;
    return r;
}

static bool __should_grant_vote(const Server& me, const MsgVoteReq& vr)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!me.nodes().get_my_node().is_voting())
        return false;

    if (vr.term < me.get_current_term())
        return false;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (me.is_already_voted())
        return false;

    /* Below we check if log is more up-to-date... */

    Index current_idx = me.log().get_current_idx();

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return true;

    bmcl::Option<const LogEntry&> e = me.log().get_at_idx(current_idx);
    assert((current_idx != 0) == e.isSome());
    if (e.isNone())
        return true;

    if (e.unwrap().term < vr.last_log_term)
        return true;

    if (vr.last_log_term == e.unwrap().term && current_idx <= vr.last_log_idx)
        return true;

    return false;
}

MsgVoteRep Server::prepare_requestvote_response_t(NodeId candidate, ReqVoteState vote)
{
    __log(candidate, "node requested vote: %d replying: %s",
        candidate,
        vote == ReqVoteState::Granted ? "granted" :
        vote == ReqVoteState::NotGranted ? "not granted" : "unknown");

    return MsgVoteRep(get_current_term(), vote);
}

MsgVoteRep Server::accept_req(NodeId nodeid, const MsgVoteReq& vr)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    if (node.isNone())
    {
        assert(false);
        /* It's possible the candidate node has been removed from the cluster but
        * hasn't received the appendentries that confirms the removal. Therefore
        * the node is partitioned and still thinks its part of the cluster. It
        * will eventually send a requestvote. This is error response tells the
        * node that it might be removed. */
        return prepare_requestvote_response_t(nodeid, ReqVoteState::UnknownNode);
    }

    if (get_current_term() < vr.term)
    {
        set_current_term(vr.term);
        become_follower();
    }

    if (!__should_grant_vote(*this, vr))
        return prepare_requestvote_response_t(nodeid, ReqVoteState::NotGranted);

    /* It shouldn't be possible for a leader or candidate to grant a vote
        * Both states would have voted for themselves */
    assert(!is_leader() && !is_candidate());

    vote_for_nodeid(nodeid);

    /* there must be in an election. */
    _me.current_leader.clear();
    _me.timeout_elapsed = std::chrono::milliseconds(0);

    return prepare_requestvote_response_t(nodeid, ReqVoteState::Granted);
}

bmcl::Option<Error> Server::accept_rep(NodeId nodeid, const MsgVoteRep& r)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);

    __log(nodeid, "node responded to requestvote status: %s",
          r.vote_granted == ReqVoteState::Granted ? "granted" :
          r.vote_granted == ReqVoteState::NotGranted ? "not granted" : "unknown");

    if (!is_candidate())
        return bmcl::None;

    if (get_current_term() < r.term)
    {
        set_current_term(r.term);
        become_follower();
        return bmcl::None;
    }

    if (get_current_term() != r.term)
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return bmcl::None;
    }

    __log(nodeid, "node responded to requestvote status:%s ct:%d rt:%d",
          r.vote_granted == ReqVoteState::Granted ? "granted" :
          r.vote_granted == ReqVoteState::NotGranted ? "not granted" : "unknown",
          _me.current_term,
          r.term);

    switch (r.vote_granted)
    {
        case ReqVoteState::Granted:
            if (node.isSome())
                node->vote_for_me(true);
            if (_nodes.votes_has_majority(_me.voted_for))
                become_leader();
            break;

        case ReqVoteState::NotGranted:
            break;

        case ReqVoteState::UnknownNode:
            if (_nodes.get_my_node().is_voting() && _me.connected == NodeStatus::Disconnecting)
                return Error::Shutdown;
            break;

        default:
            assert(0);
    }

    return bmcl::None;
}

bmcl::Result<MsgAddEntryRep, Error> Server::accept_entry(const MsgAddEntryReq& e)
{
    if (!is_leader())
        return Error::NotLeader;

    __log(_nodes.get_my_id(), "received entry t:%d id: %d idx: %d", _me.current_term, e.id, _log.get_current_idx() + 1);

    LogEntry ety = e;
    ety.term = _me.current_term;
    auto r = entry_append(ety);
    if (r.isSome())
        return r.unwrap();

    /* if we're the only node, we can consider the entry committed */
    if (1 == _nodes.get_num_voting_nodes())
        _log.commit_all();

    for (const Node& i: _nodes.items())
    {
        if (_nodes.is_me(i.get_id()) || !i.is_voting())
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        Index next_idx = i.get_next_idx();
        if (next_idx == _log.get_current_idx())
            send_appendentries(i);
    }

    return MsgAddEntryRep(_me.current_term, e.id, _log.get_current_idx());
}

void Server::entry_append_impl(const LogEntry& ety, Index idx)
{
    if (!ety.is_cfg_change())
        return;

    assert(ety.node.isSome());
    NodeId id = ety.node.unwrap();
    bmcl::Option<Node&> node = _nodes.get_node(id);

    switch (ety.type)
    {
    case LotType::AddNonVotingNode:
        if (!_nodes.is_me(id))
        {
            bmcl::Option<Node&> node = _nodes.add_non_voting_node(id);
            assert(node.isSome());
        }
        break;

    case LotType::AddNode:
        node = _nodes.add_node(id);
        assert(node.isSome());
        assert(node->is_voting());
        break;

    case LotType::DemoteNode:
        node->set_voting(false);
        break;

    case LotType::RemoveNode:
        if (node.isSome())
            _nodes.remove_node(node->get_id());
        break;

    default:
        assert(0);
    }
}

void Server::entry_apply_node_add(const LogEntry& ety, NodeId id)
{
    bmcl::Option<Node&> node = _nodes.get_node(id);
    assert(node.isSome());
    if (node.isSome())
    {
        node->set_has_sufficient_logs();
        if (_nodes.is_me(node->get_id()))
            _me.connected = NodeStatus::Connected;
    }
}

void Server::pop_log(const LogEntry& ety, const Index idx)
{
    if (!ety.is_cfg_change())
        return;

    assert(ety.node.isSome());
    NodeId id = ety.node.unwrap();

    switch (ety.type)
    {
    case LotType::DemoteNode:
    {
        bmcl::Option<Node&> node = _nodes.get_node(id);
        node->set_voting(true);
    }
    break;

    case LotType::RemoveNode:
    {
        bmcl::Option<Node&> node = _nodes.add_non_voting_node(id);
        assert(node.isSome());
    }
    break;

    case LotType::AddNonVotingNode:
    {
        _nodes.remove_node(id);
    }
    break;

    case LotType::AddNode:
    {
        bmcl::Option<Node&> node = _nodes.get_node(id);
        node->set_voting(false);
    }
    break;

    default:
        assert(false);
        break;
    }
}

bmcl::Option<Error> Server::entry_append(const LogEntry& ety)
{
    auto e = _log.entry_append(ety);
    if (e.isSome())
        return e;
    entry_append_impl(ety, _log.get_current_idx());
    return bmcl::None;
}

bmcl::Option<Error> Server::send_appendentries(NodeId node)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return send_appendentries(n.unwrap());
}

bmcl::Option<Error> Server::send_appendentries(const Node& node)
{
    assert(!_nodes.is_me(node.get_id()));

    MsgAppendEntriesReq ae(_me.current_term, 0, TermId(0), _log.get_commit_idx());
    Index next_idx = node.get_next_idx();

    ae.entries = _log.get_from_idx(next_idx, &ae.n_entries).unwrapOr(nullptr);

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        bmcl::Option<const LogEntry&> prev_ety = _log.get_at_idx(next_idx - 1);
        ae.prev_log_idx = next_idx - 1;
        if (prev_ety.isSome())
            ae.prev_log_term = prev_ety.unwrap().term;
    }

    __log(node.get_id(), "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          _log.get_current_idx(),
          _log.get_commit_idx(),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);

    return _sender->append_entries(node.get_id(), ae);
}

void Server::send_appendentries_all()
{
    for (const Node& i: _nodes.items())
    {
        if (_nodes.is_me(i.get_id()))
            continue;
        send_appendentries(i);
    }
}

void Server::vote_for_nodeid(NodeId nodeid)
{
    _me.voted_for = nodeid;
    _saver->persist_vote(nodeid);
}

void Server::set_current_term(TermId term)
{
    if (_me.current_term < term)
    {
        _me.current_term = term;
        _me.voted_for.clear();
        _saver->persist_term(term);
    }
}

void Server::set_state(State state)
{
    /* if became the leader, then update the current leader entry */
    if (state == State::Leader)
        _me.current_leader = _nodes.get_my_id();
    _me.state = state;
}

}