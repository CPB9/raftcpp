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

void Server::__log(const bmcl::Option<Node&> node, const char *fmt, ...)
{
    if (!_me.cb.log)
        return;

    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    _me.cb.log(this, node, buf);
}

void Server::__log(const bmcl::Option<const Node&> node, const char *fmt, ...) const
{
    if (!_me.cb.log)
        return;

    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    _me.cb.log(this, node, buf);
}

Server::Server(node_id id, bool is_voting, const raft_cbs_t& funcs) : _nodes(id, is_voting), _log(this)
{
    _me.cb = { 0 };

    _me.current_term = 0;
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    _me.request_timeout = std::chrono::milliseconds(200);
    _me.election_timeout = std::chrono::milliseconds(1000);
    set_state(raft_state_e::FOLLOWER);
    set_callbacks(funcs);
}

void Server::election_start()
{
    __log(NULL, "election starting: %d %d, term: %d ci: %d",
          _me.election_timeout.count(), _me.timeout_elapsed.count(), _me.current_term,
          _log.get_current_idx());

    become_candidate();
}

void Server::become_leader()
{
    __log(NULL, "becoming leader term:%d", get_current_term());

    set_state(raft_state_e::LEADER);
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
    __log(NULL, "becoming candidate");

    set_current_term(get_current_term() + 1);
    _nodes.reset_all_votes();
    vote_for_nodeid(_nodes.get_my_id());
    _me.current_leader.clear();
    set_state(raft_state_e::CANDIDATE);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    _me.timeout_elapsed = std::chrono::milliseconds(_me.election_timeout.count() - 2 * (rand() % _me.election_timeout.count()));

    _me.cb.send_requestvote(this, msg_requestvote_t(_me.current_term, _log.get_current_idx(), _log.get_last_log_term().unwrapOr(0)));
}

void Server::become_follower()
{
    __log(NULL, "becoming follower");
    set_state(raft_state_e::FOLLOWER);
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
            send_appendentries_all();
    }
    else if (_me.election_timeout <= _me.timeout_elapsed)
    {
        if (1 < _nodes.get_num_voting_nodes())
        {
            const Node& node = _nodes.get_my_node();
            if (node.is_voting())
                election_start();
        }
    }

    if (_log.has_not_applied())
        return _log.entry_apply_one();

    return bmcl::None;
}

bmcl::Option<Error> Server::accept_appendentries_response(node_id nodeid, const msg_appendentries_response_t& r)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    __log(node,
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
        std::size_t next_idx = node->get_next_idx();
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
        _me.cb.node_has_sufficient_logs &&
        false == node->has_sufficient_logs()
        )
    {
        bool e = _me.cb.node_has_sufficient_logs(this, node.unwrap());
        if (e)
            node->set_has_sufficient_logs();
    }

    /* Update commit idx */
    std::size_t point = r.current_idx;
    if (point > 0)
    {
        bmcl::Option<const raft_entry_t&> ety = _log.get_at_idx(point);
        assert(ety.isSome());
        if (!_log.is_committed(point) && ety.unwrap().term == _me.current_term)
        {
            if (_nodes.is_committed(point))
                _log.set_commit_idx(point);
        }
    }

    /* Aggressively send remaining entries */
    if (_log.get_at_idx(node->get_next_idx()).isSome())
        send_appendentries(node.unwrap());

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

bmcl::Result<msg_appendentries_response_t, Error> Server::accept_appendentries(node_id nodeid, const msg_appendentries_t& ae)
{
    msg_appendentries_response_t r(_me.current_term, false, 0, 0);
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    if (node.isNone())
        return Error::NodeUnknown;

    _me.timeout_elapsed = std::chrono::milliseconds(0);

    if (0 < ae.n_entries)
        __log(node, "recvd appendentries t:%d ci:%d lc:%d pli:%d plt:%d #%d",
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
        __log(node, "AE term %d is less than current term %d", ae.term, _me.current_term);
        return msg_appendentries_response_t(r.term, false, _log.get_current_idx(), 0);
    }

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae.prev_log_idx)
    {
        bmcl::Option<const raft_entry_t&> e = _log.get_at_idx(ae.prev_log_idx);
        if (e.isNone() || _log.get_current_idx() < ae.prev_log_idx)
        {
            /* 2. Reply false if log doesn't contain an entry at prevLogIndex
            whose term matches prevLogTerm (§5.3) */
            __log(node, "AE no log at prev_idx %d", ae.prev_log_idx);
            return msg_appendentries_response_t(r.term, false, _log.get_current_idx(), 0);
        }

        if (e.unwrap().term != ae.prev_log_term)
        {
            __log(node, "AE term doesn't match prev_term (ie. %d vs %d) ci:%d pli:%d", e.unwrap().term, ae.prev_log_term, _log.get_current_idx(), ae.prev_log_idx);
            /* Delete all the following log entries because they don't match */
            _log.entry_delete_from_idx(ae.prev_log_idx);
            return msg_appendentries_response_t(r.term, false, ae.prev_log_idx - 1, 0);
        }
    }

    r.current_idx = ae.prev_log_idx;

    std::size_t i;
    for (i = 0; i < ae.n_entries; i++)
    {
        const raft_entry_t* ety = &ae.entries[i];
        std::size_t ety_index = ae.prev_log_idx + 1 + i;
        r.current_idx = ety_index;
        bmcl::Option<const raft_entry_t&> existing_ety = _log.get_at_idx(ety_index);
        if (existing_ety.isNone())
            break;
        //assert((existing_ety.unwrap().term == ety->term) == _log.is_committed(ety_index));
        if (existing_ety.unwrap().term != ety->term && !_log.is_committed(ety_index))
        {
            /* 3. If an existing entry conflicts with a new one (same index
            but different terms), delete the existing entry and all that
            follow it (§5.3) */
            _log.entry_delete_from_idx(ety_index);
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
            return msg_appendentries_response_t(r.term, false, _log.get_current_idx(), 0);
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

static bool __should_grant_vote(Server* me, const msg_requestvote_t& vr)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!me->nodes().get_my_node().is_voting())
        return false;

    if (vr.term < me->get_current_term())
        return false;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (me->is_already_voted())
        return false;

    /* Below we check if log is more up-to-date... */

    std::size_t current_idx = me->log().get_current_idx();

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return true;

    bmcl::Option<const raft_entry_t&> e = me->log().get_at_idx(current_idx);
    assert((current_idx != 0) == e.isSome());
    if (e.isNone())
        return true;


    if (e.unwrap().term < vr.last_log_term)
        return true;

    if (vr.last_log_term == e.unwrap().term && current_idx <= vr.last_log_idx)
        return true;

    return false;
}

msg_requestvote_response_t Server::prepare_requestvote_response_t(node_id candidate, raft_request_vote vote)
{
    __log(_nodes.get_node(candidate), "node requested vote: %d replying: %s",
        candidate,
        vote == raft_request_vote::GRANTED ? "granted" :
        vote == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown");

    return msg_requestvote_response_t(get_current_term(), vote);
}

msg_requestvote_response_t Server::accept_requestvote(node_id nodeid, const msg_requestvote_t& vr)
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
        return prepare_requestvote_response_t(nodeid, raft_request_vote::UNKNOWN_NODE);
    }

    if (get_current_term() < vr.term)
    {
        set_current_term(vr.term);
        become_follower();
    }

    if (!__should_grant_vote(this, vr))
        return prepare_requestvote_response_t(nodeid, raft_request_vote::NOT_GRANTED);

    /* It shouldn't be possible for a leader or candidate to grant a vote
        * Both states would have voted for themselves */
    assert(!is_leader() && !is_candidate());

    vote_for_nodeid(nodeid);

    /* there must be in an election. */
    _me.current_leader.clear();
    _me.timeout_elapsed = std::chrono::milliseconds(0);

    return prepare_requestvote_response_t(nodeid, raft_request_vote::GRANTED);
}

bmcl::Option<Error> Server::accept_requestvote_response(node_id nodeid, const msg_requestvote_response_t& r)
{
    bmcl::Option<Node&> node = _nodes.get_node(nodeid);

    __log(node, "node responded to requestvote status: %s",
          r.vote_granted == raft_request_vote::GRANTED ? "granted" :
          r.vote_granted == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown");

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

    __log(node, "node responded to requestvote status:%s ct:%d rt:%d",
          r.vote_granted == raft_request_vote::GRANTED ? "granted" :
          r.vote_granted == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown",
          _me.current_term,
          r.term);

    switch (r.vote_granted)
    {
        case raft_request_vote::GRANTED:
            if (node.isSome())
                node->vote_for_me(true);
            if (_nodes.raft_votes_is_majority(_me.voted_for))
                become_leader();
            break;

        case raft_request_vote::NOT_GRANTED:
            break;

        case raft_request_vote::UNKNOWN_NODE:
            if (_nodes.get_my_node().is_voting() && _me.connected == node_status::DISCONNECTING)
                return Error::Shutdown;
            break;

        default:
            assert(0);
    }

    return bmcl::None;
}

bmcl::Result<msg_entry_response_t, Error> Server::accept_entry(const msg_entry_t& e)
{
    if (!is_leader())
        return Error::NotLeader;

    __log(NULL, "received entry t:%d id: %d idx: %d", _me.current_term, e.id, _log.get_current_idx() + 1);

    raft_entry_t ety = e;
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
        std::size_t next_idx = i.get_next_idx();
        if (next_idx == _log.get_current_idx())
            send_appendentries(i);
    }

    return msg_entry_response_t(_me.current_term, e.id, _log.get_current_idx());
}

void Server::entry_append_impl(const raft_entry_t& ety, const std::size_t idx)
{
    if (!ety.is_cfg_change())
        return;

    if (!_me.cb.log_get_node_id)
    {
        //assert(false);
        return;
    }

    node_id id = (node_id)_me.cb.log_get_node_id(this, ety, idx);
    bmcl::Option<Node&> node = _nodes.get_node(id);

    switch (ety.type)
    {
    case logtype_e::ADD_NONVOTING_NODE:
        if (!_nodes.is_me(id))
        {
            bmcl::Option<Node&> node = _nodes.add_non_voting_node(id);
            assert(node.isSome());
        }
        break;

    case logtype_e::ADD_NODE:
        node = _nodes.add_node(id);
        assert(node.isSome());
        assert(node->is_voting());
        break;

    case logtype_e::DEMOTE_NODE:
        node->set_voting(false);
        break;

    case logtype_e::REMOVE_NODE:
        if (node.isSome())
            _nodes.remove_node(node->get_id());
        break;

    default:
        assert(0);
    }
}

void Server::entry_apply_node_add(const raft_entry_t& ety, node_id id)
{
    bmcl::Option<Node&> node = _nodes.get_node(id);
    assert(node.isSome());
    if (node.isSome())
    {
        node->set_has_sufficient_logs();
        if (_nodes.is_me(node->get_id()))
            _me.connected = node_status::CONNECTED;
    }
}


void Server::pop_log(const raft_entry_t& ety, const std::size_t idx)
{
    if (!ety.is_cfg_change())
        return;

    node_id id = (node_id)_me.cb.log_get_node_id(this, ety, idx);

    switch (ety.type)
    {
    case logtype_e::DEMOTE_NODE:
    {
        bmcl::Option<Node&> node = _nodes.get_node(id);
        node->set_voting(true);
    }
    break;

    case logtype_e::REMOVE_NODE:
    {
        bmcl::Option<Node&> node = _nodes.add_non_voting_node(id);
        assert(node.isSome());
    }
    break;

    case logtype_e::ADD_NONVOTING_NODE:
    {
        _nodes.remove_node(id);
    }
    break;

    case logtype_e::ADD_NODE:
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

bmcl::Option<Error> Server::entry_append(const raft_entry_t& ety)
{
    auto e = _log.entry_append(ety);
    if (e.isSome())
        return e;
    entry_append_impl(ety, _log.get_current_idx());
    return bmcl::None;
}

bmcl::Option<Error> Server::send_appendentries(const bmcl::Option<node_id>& node)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return send_appendentries(n.unwrap());
}

bmcl::Option<Error> Server::send_appendentries(const Node& node)
{
    assert(!_nodes.is_me(node.get_id()));

    msg_appendentries_t ae = {0};
    ae.term = _me.current_term;
    ae.leader_commit = _log.get_commit_idx();
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    std::size_t next_idx = node.get_next_idx();

    ae.entries = _log.get_from_idx(next_idx, &ae.n_entries).unwrapOr(nullptr);

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        bmcl::Option<const raft_entry_t&> prev_ety = _log.get_at_idx(next_idx - 1);
        ae.prev_log_idx = next_idx - 1;
        if (prev_ety.isSome())
            ae.prev_log_term = prev_ety.unwrap().term;
    }

    __log(node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          _log.get_current_idx(),
          _log.get_commit_idx(),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);

    assert(_me.cb.send_appendentries);
    return _me.cb.send_appendentries(this, node, ae);
}

void Server::send_appendentries_all()
{
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    for (const Node& i: _nodes.items())
    {
        if (_nodes.is_me(i.get_id()))
            continue;
        send_appendentries(i);
    }
}

void Server::vote_for_nodeid(node_id nodeid)
{
    _me.voted_for = nodeid;
    assert(_me.cb.persist_vote);
    _me.cb.persist_vote(this, (std::size_t)nodeid);
}

raft_entry_state_e Server::entry_get_state(const msg_entry_response_t& r) const
{
    bmcl::Option<const raft_entry_t&> ety = _log.get_at_idx(r.idx);
    if (ety.isNone())
        return raft_entry_state_e::NOTCOMMITTED;

    /* entry from another leader has invalidated this entry message */
    if (r.term != ety.unwrap().term)
        return raft_entry_state_e::INVALIDATED;
    return _log.is_committed(r.idx) ? raft_entry_state_e::COMMITTED : raft_entry_state_e::NOTCOMMITTED;
}

void Server::set_current_term(std::size_t term)
{
    if (_me.current_term < term)
    {
        _me.current_term = term;
        _me.voted_for.clear();
        assert(_me.cb.persist_term);
        _me.cb.persist_term(this, term);
    }
}

void Server::set_state(raft_state_e state)
{
    /* if became the leader, then update the current leader entry */
    if (state == raft_state_e::LEADER)
        _me.current_leader = _nodes.get_my_id();
    _me.state = state;
}

}