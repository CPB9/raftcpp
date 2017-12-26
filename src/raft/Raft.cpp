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
#include <random>

/* for varags */
#include <stdarg.h>

#include "Raft.h"
#include "Log.h"
#include "Node.h"

namespace raft
{

Timer::Timer()
{
    timeout_elapsed = std::chrono::milliseconds(0);
    set_timeout(std::chrono::milliseconds(200), 5);
    randomize_election_timeout();
}

void Timer::set_timeout(std::chrono::milliseconds msec, std::size_t factor)
{
    request_timeout = msec;
    election_timeout = msec * factor;
    randomize_election_timeout();
}
void Timer::randomize_election_timeout()
{
    /* [election_timeout, 2 * election_timeout) */
    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<> distr(election_timeout.count(), 2*election_timeout.count()); // define the range
    election_timeout_rand = std::chrono::milliseconds(distr(eng));
}

static inline const char* vote_to_str(bool vote) { return vote ? "granted" : "not granted"; }

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
    become_follower();
}

void Server::become_leader()
{
    __log(_nodes.get_my_id(), "becoming leader term:%d", get_current_term());

    set_state(State::Leader);
    _timer.reset_elapsed();

    for (const Node& i: _nodes.items())
    {
        Node& n = _nodes.get_node(i.get_id()).unwrap();
        n.set_next_idx(_log.get_current_idx() + 1);
        n.set_match_idx(0);
        n.set_need_vote_req(false);
        send_appendentries(n, _sender);
    }
}

void Server::become_candidate()
{
    set_current_term(get_current_term() + 1);
    _nodes.reset_all_votes();
    vote_for_nodeid(_nodes.get_my_id());
    _me.current_leader.clear();
    set_state(State::Candidate);
    _timer.randomize_election_timeout();
    _timer.reset_elapsed();
    _nodes.set_all_need_pings(false);

    __log(_nodes.get_my_id(), "becoming candidate");
    __log(_nodes.get_my_id(), "randomize election timeout to %d", _timer.get_election_timeout_rand());
    for (const Node& i : _nodes.items())
    {
        Node& n = _nodes.get_node(i.get_id()).unwrap();
        send_reqvote(n, _sender);
    }
}

void Server::become_follower()
{
    set_state(State::Follower);
    _timer.randomize_election_timeout();
    _timer.reset_elapsed();
    _nodes.set_all_need_vote_req(false);
    _nodes.set_all_need_pings(false);
    __log(_nodes.get_my_id(), "becoming follower");
    __log(_nodes.get_my_id(), "randomize election timeout to %d", _timer.get_election_timeout_rand());
}

bmcl::Option<Error> Server::raft_periodic(std::chrono::milliseconds msec_since_last_period)
{
    _timer.add_elapsed(msec_since_last_period);

    /* Only one voting node means it's safe for us to become the leader */
    if (1 == _nodes.get_num_voting_nodes())
    {
        const Node& node = _nodes.get_my_node();
        if (node.is_voting() && !is_leader())
        {
            vote_for_nodeid(node.get_id());
            become_leader();
        }
    }

    if (is_leader())
    {
        if (_timer.is_time_to_ping())
        {
            for (const Node& i : _nodes.items())
            {
                send_appendentries(_nodes.get_node(i.get_id()).unwrap(), _sender);
            }
            _timer.reset_elapsed();
        }
    }
    else if (_timer.is_time_to_elect())
    {
        if (1 < _nodes.get_num_voting_nodes())
        {
            const Node& node = _nodes.get_my_node();
            if (node.is_voting())
            {
                __log(_nodes.get_my_id(), "election starting: %d %d, term: %d ci: %d",
                    _timer.get_election_timeout_rand().count(), _timer.get_timeout_elapsed(), _me.current_term,
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

    if (!is_leader())
        return Error::NotLeader;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (_me.current_term < r.term)
    {
        set_current_term(r.term);
        become_follower();
        _me.current_leader.clear();
        return bmcl::None;
    }

    if (_me.current_term > r.term)
        return bmcl::None;

    /* Stale response -- ignore */
    if (r.current_idx != 0 && r.current_idx <= node->get_match_idx())
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
        send_appendentries(node.unwrap(), _sender);
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
        send_appendentries(node.unwrap(), _sender);

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

bmcl::Result<MsgAppendEntriesRep, Error> Server::accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae)
{
    MsgAppendEntriesRep r(_me.current_term, false, 0, 0);
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    if (node.isNone())
        return Error::NodeUnknown;

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

    /* update current leader because ae->term is up to date */
    _me.current_leader = node->get_id();

    _timer.reset_elapsed();

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae.prev_log_idx)
    {
        bmcl::Option<const LogEntry&> e = _log.get_at_idx(ae.prev_log_idx);
        if (e.isNone())
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

MsgVoteRep Server::prepare_requestvote_response_t(NodeId candidate, bool vote)
{
    __log(candidate, "node requested vote: %d replying: %s", candidate, vote_to_str(vote));
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
        return prepare_requestvote_response_t(nodeid, false);
    }

    if (get_current_term() < vr.term)
    {
        set_current_term(vr.term);
        become_follower();
        _me.current_leader = node->get_id();
    }

    if (!__should_grant_vote(*this, vr))
        return prepare_requestvote_response_t(nodeid, false);

    /* It shouldn't be possible for a leader or candidate to grant a vote
        * Both states would have voted for themselves */
    assert(!is_leader() && !is_candidate());

    vote_for_nodeid(nodeid);

    /* there must be in an election. */
    _me.current_leader.clear();
    _timer.reset_elapsed();

    return prepare_requestvote_response_t(nodeid, true);
}

bmcl::Option<Error> Server::accept_rep(NodeId nodeid, const MsgVoteRep& r)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);

    __log(nodeid, "node responded to requestvote status: %s", vote_to_str(r.vote_granted));

    if (!is_candidate())
        return bmcl::None;

    if (get_current_term() < r.term)
    {
        set_current_term(r.term);
        become_follower();
        _me.current_leader.clear();
        return bmcl::None;
    }

    if (get_current_term() != r.term)
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return bmcl::None;
    }

    __log(nodeid, "node responded to requestvote status:%s ct:%d rt:%d", vote_to_str(r.vote_granted), _me.current_term, r.term);

    if (r.vote_granted)
    {
        if (node.isSome())
            node->vote_for_me(true);
        if (_nodes.votes_has_majority(_me.voted_for))
            become_leader();
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
        {
            Node& n = _nodes.get_node(i.get_id()).unwrap();
            send_appendentries(n, _sender);
        }
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
        if (!_nodes.is_me(id) && node.isNone())
        {
            const Node& n = _nodes.add_non_voting_node(id);
            assert(!n.is_voting());
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
        const Node& node = _nodes.add_non_voting_node(id);
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

bmcl::Option<Error> Server::send_smth_for(NodeId node, ISender* sender)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;

    bmcl::Option<Error> e;
    if (n->need_vote_req())
    {
        e = send_reqvote(n.unwrap(), sender);
        n->set_need_vote_req(false);
        return e;
    }

    if (n->need_append_endtries_req())
    {
        e = send_appendentries(n.unwrap(), sender);
        n->set_need_append_endtries_req(false);
        return e;
    }

    return Error::NothingToSend;
}

bmcl::Option<Error> Server::send_reqvote(Node& node, ISender* sender)
{
    if (_nodes.is_me(node.get_id()))
        return Error::CantSendToMyself;

    if (!is_candidate())
        return Error::NotCandidate;

    if (!sender)
    {
        node.set_need_vote_req(true);
        return bmcl::None;
    }
    return sender->request_vote(node.get_id(), MsgVoteReq(_me.current_term, _log.get_current_idx(), _log.get_last_log_term().unwrapOr(TermId(0))));
}

bmcl::Option<Error> Server::send_appendentries(NodeId node)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return send_appendentries(n.unwrap(), _sender);
}

bmcl::Option<Error> Server::send_appendentries(Node& node, ISender* sender)
{
    if (_nodes.is_me(node.get_id()))
        return Error::CantSendToMyself;

    if (!is_leader())
        return Error::NotLeader;

    if (!sender)
    {
        node.set_need_append_endtries_req(true);
        return bmcl::None;
    }

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

    return sender->append_entries(node.get_id(), ae);
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