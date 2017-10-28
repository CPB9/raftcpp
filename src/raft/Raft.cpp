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

Server::Server(node_id id, bool is_voting, const raft_cbs_t& funcs)
{
    _me.cb = { 0 };
    _me.commit_idx = 0;
    _me.last_applied_idx = 0;

    _me.current_term = 0;
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    _me.request_timeout = std::chrono::milliseconds(200);
    _me.election_timeout = std::chrono::milliseconds(1000);
    set_state(raft_state_e::FOLLOWER);

    auto r = add_node(id);
    assert(r.isSome());
    r.unwrap().set_voting(is_voting);
    _me.node = r.unwrap().get_id();
    set_callbacks(funcs);
}

void Server::election_start()
{
    __log(NULL, "election starting: %d %d, term: %d ci: %d",
          _me.election_timeout.count(), _me.timeout_elapsed.count(), _me.current_term,
          get_current_idx());

    become_candidate();
}

void Server::become_leader()
{
    __log(NULL, "becoming leader term:%d", get_current_term());

    set_state(raft_state_e::LEADER);
    for (Node& i: _me.nodes)
    {
        if (_me.node == i.get_id())
            continue;
        i.set_next_idx(get_current_idx() + 1);
        i.set_match_idx(0);
        raft_send_appendentries(i);
    }
}

void Server::become_candidate()
{
    __log(NULL, "becoming candidate");

    set_current_term(get_current_term() + 1);
    for (Node& i : _me.nodes)
        i.vote_for_me(false);
    vote_for_nodeid(_me.node);
    _me.current_leader.clear();
    set_state(raft_state_e::CANDIDATE);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    _me.timeout_elapsed = std::chrono::milliseconds(_me.election_timeout.count() - 2 * (rand() % _me.election_timeout.count()));

    for (const Node& i : _me.nodes)
        if (_me.node != i.get_id() && i.is_voting())
            raft_send_requestvote(i);
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
    if (1 == get_num_voting_nodes())
    {
        const Node& node = get_my_node();
        if (node.is_voting() && !is_leader())
            become_leader();
    }

    if (_me.state == raft_state_e::LEADER)
    {
        if (_me.request_timeout <= _me.timeout_elapsed)
            raft_send_appendentries_all();
    }
    else if (_me.election_timeout <= _me.timeout_elapsed)
    {
        if (1 < get_num_voting_nodes())
        {
            const Node& node = get_my_node();
            if (node.is_voting())
                election_start();
        }
    }

    if (_me.last_applied_idx < _me.commit_idx)
        return raft_apply_entry();

    return bmcl::None;
}

bmcl::Option<Error> Server::raft_recv_appendentries_response(bmcl::Option<node_id> nodeid, const msg_appendentries_response_t& r)
{
    bmcl::Option<Node&> node = get_node(nodeid);
    __log(node,
          "received appendentries response %s ci:%d rci:%d 1stidx:%d",
          r.success ? "SUCCESS" : "fail",
          get_current_idx(),
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
    else if (_me.current_term != r.term)
        return bmcl::None;

    if (!r.success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        std::size_t next_idx = node->get_next_idx();
        assert(0 < next_idx);
        if (r.current_idx < next_idx - 1)
            node->set_next_idx(std::min(r.current_idx + 1, get_current_idx()));
        else
            node->set_next_idx(next_idx - 1);

        /* retry */
        raft_send_appendentries(node.unwrap());
        return bmcl::None;
    }

    assert(r.current_idx <= get_current_idx());

    node->set_next_idx(r.current_idx + 1);
    node->set_match_idx(r.current_idx);

    if (!node->is_voting() &&
        !voting_change_is_in_progress() &&
        get_current_idx() <= r.current_idx + 1 &&
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
    if (point)
    {
        bmcl::Option<const raft_entry_t&> ety = get_entry_from_idx(point);
        assert(ety.isSome());
        if (get_commit_idx() < point && ety.unwrap().term == _me.current_term)
        {
            std::size_t votes = 1;
            for (const Node& i: _me.nodes)
            {
                if (_me.node != i.get_id() && i.is_voting() && point <= i.get_match_idx())
                {
                    votes++;
                }
            }

            if (get_num_voting_nodes() / 2 < votes)
                set_commit_idx(point);
        }
    }

    /* Aggressively send remaining entries */
    if (get_entry_from_idx(node->get_next_idx()).isSome())
        raft_send_appendentries(node.unwrap());

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

bmcl::Result<msg_appendentries_response_t, Error> Server::raft_recv_appendentries(bmcl::Option<node_id> nodeid, const msg_appendentries_t& ae)
{
    msg_appendentries_response_t r = {0};
    bmcl::Option<Node&> node = get_node(nodeid);

    _me.timeout_elapsed = std::chrono::milliseconds(0);

    if (0 < ae.n_entries)
        __log(node, "recvd appendentries t:%d ci:%d lc:%d pli:%d plt:%d #%d",
              ae.term,
              get_current_idx(),
              ae.leader_commit,
              ae.prev_log_idx,
              ae.prev_log_term,
              ae.n_entries);

    r.term = _me.current_term;

    if (is_candidate() && _me.current_term == ae.term)
    {
        become_follower();
    }
    else if (_me.current_term < ae.term)
    {
        set_current_term(ae.term);
        r.term = ae.term;
        become_follower();
    }
    else if (ae.term < _me.current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        __log(node, "AE term %d is less than current term %d",
              ae.term, _me.current_term);
        goto fail_with_current_idx;
    }

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae.prev_log_idx)
    {
        bmcl::Option<const raft_entry_t&> e = get_entry_from_idx(ae.prev_log_idx);
        if (e.isNone())
        {
            __log(node, "AE no log at prev_idx %d", ae.prev_log_idx);
            goto fail_with_current_idx;
        }

        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        if (get_current_idx() < ae.prev_log_idx)
            goto fail_with_current_idx;

        if (e.unwrap().term != ae.prev_log_term)
        {
            __log(node, "AE term doesn't match prev_term (ie. %d vs %d) ci:%d pli:%d",
                e.unwrap().term, ae.prev_log_term, get_current_idx(), ae.prev_log_idx);
            /* Delete all the following log entries because they don't match */
            delete_entry_from_idx(ae.prev_log_idx);
            r.current_idx = ae.prev_log_idx - 1;
            goto fail;
        }
    }

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    if (0 < ae.prev_log_idx && ae.prev_log_idx + 1 < get_current_idx())
    {
        /* Heartbeats shouldn't cause logs to be deleted. Heartbeats might be
        * sent before the leader received the last appendentries response */
        if (ae.n_entries != 0 &&
            /* this is an old out-of-order appendentry message */
            _me.commit_idx < ae.prev_log_idx + 1)
            delete_entry_from_idx(ae.prev_log_idx + 1);
    }

    r.current_idx = ae.prev_log_idx;

    std::size_t i;
    for (i = 0; i < ae.n_entries; i++)
    {
        const raft_entry_t* ety = &ae.entries[i];
        std::size_t ety_index = ae.prev_log_idx + 1 + i;
        bmcl::Option<const raft_entry_t&> existing_ety = get_entry_from_idx(ety_index);
        r.current_idx = ety_index;
        if (existing_ety.isSome() && existing_ety.unwrap().term != ety->term && _me.commit_idx < ety_index)
        {
            delete_entry_from_idx(ety_index);
            break;
        }
        else if (existing_ety.isNone())
            break;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < ae.n_entries; i++)
    {
        bmcl::Option<Error> e = append_entry(ae.entries[i]);
        if (e.isSome())
        {
            if (e.unwrap() == Error::Shutdown)
            {
                r.success = false;
                r.first_idx = 0;
                return Error::Shutdown;
            }
            goto fail_with_current_idx;
        }
        r.current_idx = ae.prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    if (get_commit_idx() < ae.leader_commit)
    {
        std::size_t last_log_idx = std::max<std::size_t>(get_current_idx(), 1);
        set_commit_idx(std::min(last_log_idx, ae.leader_commit));
    }

    /* update current leader because we accepted appendentries from it */
    _me.current_leader = nodeid;

    r.success = true;
    r.first_idx = ae.prev_log_idx + 1;
    return r;

fail_with_current_idx:
    r.current_idx = get_current_idx();
fail:
    r.success = false;
    r.first_idx = 0;
    return r;
}

static bool __should_grant_vote(Server* me, const msg_requestvote_t& vr)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!me->get_my_node().is_voting())
        return false;

    if (vr.term < me->get_current_term())
        return false;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (me->is_already_voted())
        return false;

    /* Below we check if log is more up-to-date... */

    std::size_t current_idx = me->get_current_idx();

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return true;

    bmcl::Option<const raft_entry_t&> e = me->get_entry_from_idx(current_idx);
    assert((current_idx != 0) == e.isSome());
    if (e.isNone())
        return true;


    if (e.unwrap().term < vr.last_log_term)
        return true;

    if (vr.last_log_term == e.unwrap().term && current_idx <= vr.last_log_idx)
        return true;

    return false;
}

bmcl::Result<msg_requestvote_response_t, Error> Server::raft_recv_requestvote(bmcl::Option<node_id> nodeid, const msg_requestvote_t& vr)
{
    msg_requestvote_response_t r = {0};
    bmcl::Option<Node&> node = get_node(nodeid);

    if (node.isNone())
        node = get_node(vr.candidate_id);

    if (get_current_term() < vr.term)
    {
        set_current_term(vr.term);
        become_follower();
    }

    if (__should_grant_vote(this, vr))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(is_leader() || is_candidate()));

        vote_for_nodeid(vr.candidate_id);
        r.vote_granted = raft_request_vote::GRANTED;

        /* there must be in an election. */
        _me.current_leader.clear();

        _me.timeout_elapsed = std::chrono::milliseconds(0);
    }
    else
    {
        /* It's possible the candidate node has been removed from the cluster but
         * hasn't received the appendentries that confirms the removal. Therefore
         * the node is partitioned and still thinks its part of the cluster. It
         * will eventually send a requestvote. This is error response tells the
         * node that it might be removed. */
        if (node.isNone())
        {
            r.vote_granted = raft_request_vote::UNKNOWN_NODE;
            goto done;
        }
        else
            r.vote_granted = raft_request_vote::NOT_GRANTED;
    }

done:
    __log(node, "node requested vote: %d replying: %s",
          node,
          r.vote_granted == raft_request_vote::GRANTED ? "granted" :
          r.vote_granted == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown");

    r.term = get_current_term();
    return r;
}

bool Server::raft_votes_is_majority(std::size_t num_nodes, std::size_t nvotes)
{
    if (num_nodes < nvotes)
        return false;
    std::size_t half = num_nodes / 2;
    return half + 1 <= nvotes;
}

bmcl::Option<Error> Server::raft_recv_requestvote_response(bmcl::Option<node_id> nodeid, const msg_requestvote_response_t& r)
{
    bmcl::Option<Node&> node = get_node(nodeid);

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
            if (Server::raft_votes_is_majority(get_num_voting_nodes(), get_nvotes_for_me()))
                become_leader();
            break;

        case raft_request_vote::NOT_GRANTED:
            break;

        case raft_request_vote::UNKNOWN_NODE:
            if (get_my_node().is_voting() && _me.connected == node_status::DISCONNECTING)
                return Error::Shutdown;
            break;

        default:
            assert(0);
    }

    return bmcl::None;
}

bmcl::Result<msg_entry_response_t, Error> Server::raft_recv_entry(const msg_entry_t& e)
{
    /* Only one voting cfg change at a time */
    if (e.is_voting_cfg_change())
        if (voting_change_is_in_progress())
            return Error::OneVotiongChangeOnly;

    if (!is_leader())
        return Error::NotLeader;

    __log(NULL, "received entry t:%d id: %d idx: %d",
        _me.current_term, e.id, get_current_idx() + 1);

    raft_entry_t ety = e;
    ety.term = _me.current_term;
    append_entry(ety);
    for (const Node& i: _me.nodes)
    {
        if (_me.node == i.get_id() || !i.is_voting())
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        std::size_t next_idx = i.get_next_idx();
        if (next_idx == get_current_idx())
            raft_send_appendentries(i);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == get_num_voting_nodes())
        set_commit_idx(get_current_idx());

    msg_entry_response_t r = {0};
    r.id = e.id;
    r.idx = get_current_idx();
    r.term = _me.current_term;

    if (e.is_voting_cfg_change())
        _me.voting_cfg_change_log_idx = get_current_idx();

    return r;
}

bmcl::Option<Error> Server::raft_send_requestvote(const bmcl::Option<node_id>& node)
{
    bmcl::Option<Node&> n = get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return raft_send_requestvote(n.unwrap());
}

bmcl::Option<Error> Server::raft_send_requestvote(const Node& node)
{
    msg_requestvote_t rv;

    assert(node.get_id() != _me.node);

    __log(node, "sending requestvote to: %d", node);

    rv.term = _me.current_term;
    rv.last_log_idx = get_current_idx();
    rv.last_log_term = get_last_log_term().unwrapOr(0);
    rv.candidate_id = get_my_nodeid();
    assert(_me.cb.send_requestvote);
    return _me.cb.send_requestvote(this, node, rv);
}

void Server::delete_entry_from_idx(std::size_t idx)
{
    assert(_me.commit_idx < idx);
    if (_me.voting_cfg_change_log_idx.isSome() && idx <= _me.voting_cfg_change_log_idx.unwrap())
        _me.voting_cfg_change_log_idx.clear();
    _me.log.log_delete(this, idx);
}

bmcl::Option<Error> Server::append_entry(const raft_entry_t& ety)
{
    if (ety.is_voting_cfg_change())
        _me.voting_cfg_change_log_idx = get_current_idx();
    return _me.log.log_append_entry(this, ety);
}

bmcl::Option<const raft_entry_t *> Server::get_entries_from_idx(std::size_t idx, std::size_t* n_etys) const
{
    return _me.log.log_get_from_idx(idx, n_etys);
}

bmcl::Option<Error> Server::raft_apply_entry()
{
    /* Don't apply after the commit_idx */
    if (_me.last_applied_idx == _me.commit_idx)
        return Error::Any;

    std::size_t log_idx = _me.last_applied_idx + 1;

    bmcl::Option<const raft_entry_t&> ety = get_entry_from_idx(log_idx);
    if (ety.isNone())
        return Error::Any;

    __log(NULL, "applying log: %d, id: %d size: %d",
        _me.last_applied_idx, ety.unwrap().id, ety.unwrap().data.len);

    _me.last_applied_idx++;
    assert(_me.cb.applylog);
    int e = _me.cb.applylog(this, ety.unwrap(), _me.last_applied_idx - 1);
    if (e == RAFT_ERR_SHUTDOWN)
        return Error::Shutdown;
    assert(e == 0);

    /* Membership Change: confirm connection with cluster */
    if (logtype_e::ADD_NODE == ety.unwrap().type)
    {
        node_id id = (node_id)_me.cb.log_get_node_id(this, ety.unwrap(), log_idx);
        bmcl::Option<Node&> node = get_node(id);
        assert(node.isSome());
        if (node.isSome())
        {
            node->set_has_sufficient_logs();
            if (node->get_id() == _me.node)
                _me.connected = node_status::CONNECTED;
        }
    }

    /* voting cfg change is now complete */
    if (_me.voting_cfg_change_log_idx.isSome() && log_idx == _me.voting_cfg_change_log_idx.unwrap())
        _me.voting_cfg_change_log_idx.clear();

    return bmcl::None;
}

bmcl::Option<Error> Server::raft_send_appendentries(const bmcl::Option<node_id>& node)
{
    bmcl::Option<Node&> n = get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return raft_send_appendentries(n.unwrap());
}

bmcl::Option<Error> Server::raft_send_appendentries(const Node& node)
{
    assert(node.get_id() != _me.node);

    msg_appendentries_t ae = {0};
    ae.term = _me.current_term;
    ae.leader_commit = get_commit_idx();
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    std::size_t next_idx = node.get_next_idx();

    ae.entries = get_entries_from_idx(next_idx, &ae.n_entries).unwrapOr(nullptr);

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        bmcl::Option<const raft_entry_t&> prev_ety = get_entry_from_idx(next_idx - 1);
        ae.prev_log_idx = next_idx - 1;
        if (prev_ety.isSome())
            ae.prev_log_term = prev_ety.unwrap().term;
    }

    __log(node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          get_current_idx(),
          get_commit_idx(),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);

    assert(_me.cb.send_appendentries);
    return _me.cb.send_appendentries(this, node, ae);
}

bmcl::Option<Error> Server::raft_send_appendentries_all()
{
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    for (const Node& i: _me.nodes)
    {
        if (_me.node != i.get_id())
        {
            bmcl::Option<Error> e = raft_send_appendentries(i);
            if (e.isSome())
                return e;
        }
    }
    return bmcl::None;
}

bmcl::Option<Node&> Server::add_node(node_id id)
{
    /* set to voting if node already exists */
    bmcl::Option<Node&> node = get_node(id);
    if (node.isSome())
    {
        if (node->is_voting())
            return bmcl::None;
        node->set_voting(true);
        return node;
    }

    _me.nodes.emplace_back(Node(id));
    return _me.nodes.back();
}

bmcl::Option<Node&> Server::add_non_voting_node(node_id id)
{
    if (get_node(id).isSome())
        return bmcl::None;

    bmcl::Option<Node&> node = add_node(id);
    if (node.isNone())
        return bmcl::None;

    node->set_voting(false);
    return node;
}

void Server::remove_node(node_id nodeid)
{
    for (std::size_t i = 0; i < _me.nodes.size(); i++)
    {
        if (_me.nodes[i].get_id() == nodeid)
        {
            _me.nodes.erase(_me.nodes.begin() + i);
            return;
        }
    }
    assert(false);
}

void Server::remove_node(const bmcl::Option<Node&>& node)
{
    if (node.isNone())
        return;
    remove_node(node->get_id());
}

std::size_t Server::get_nvotes_for_me() const
{
    std::size_t votes = 0;

    for (const Node& i: _me.nodes)
    {
        if (_me.node != i.get_id() && i.is_voting() && i.has_vote_for_me())
            votes += 1;
    }

    if (_me.voted_for == _me.node)
        votes += 1;

    return votes;
}

void Server::vote_for_nodeid(bmcl::Option<node_id> nodeid)
{
    _me.voted_for = nodeid;
    assert(_me.cb.persist_vote);
    int id = -1;
    if (nodeid.isSome()) id = (int)nodeid.unwrap();
    _me.cb.persist_vote(this, id);
}

int Server::msg_entry_response_committed(const msg_entry_response_t& r) const
{
    bmcl::Option<const raft_entry_t&> ety = get_entry_from_idx(r.idx);
    if (ety.isNone())
        return 0;

    /* entry from another leader has invalidated this entry message */
    if (r.term != ety.unwrap().term)
        return -1;
    return r.idx <= get_commit_idx();
}

bmcl::Option<Error> Server::apply_all()
{
    while (get_last_applied_idx() < get_commit_idx())
    {
        bmcl::Option<Error> e = raft_apply_entry();
        if (e.isSome())
            return e;
    }

    return bmcl::None;
}

void Server::offer_log(const raft_entry_t& ety, const std::size_t idx)
{
    if (!ety.is_cfg_change())
        return;

    node_id id = (node_id)_me.cb.log_get_node_id(this, ety, idx);
    bmcl::Option<Node&> node = get_node(id);
    bool is_self = is_my_node(id);

    switch (ety.type)
    {
    case logtype_e::ADD_NONVOTING_NODE:
            if (!is_self)
            {
                bmcl::Option<Node&> node = add_non_voting_node(id);
                assert(node.isSome());
            }
            break;

        case logtype_e::ADD_NODE:
            node = add_node(id);
            assert(node.isSome());
            assert(node->is_voting());
            break;

        case logtype_e::DEMOTE_NODE:
            node->set_voting(false);
            break;

        case logtype_e::REMOVE_NODE:
            if (node.isSome())
                remove_node(node->get_id());
            break;

        default:
            assert(0);
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
                bmcl::Option<Node&> node = get_node(id);
                node->set_voting(true);
            }
            break;

        case logtype_e::REMOVE_NODE:
            {
                bmcl::Option<Node&> node = add_non_voting_node(id);
                assert(node.isSome());
            }
            break;

        case logtype_e::ADD_NONVOTING_NODE:
            {
                bool is_self = is_my_node(id);
                remove_node(id);
                assert(!is_self);
            }
            break;

        case logtype_e::ADD_NODE:
            {
                bmcl::Option<Node&> node = get_node(id);
                node->set_voting(false);
            }
            break;

        default:
            assert(false);
            break;
    }
}


//properties

std::size_t Server::get_num_voting_nodes() const
{
    std::size_t num = 0;
    for (const Node& i: _me.nodes)
        if (i.is_voting())
            num++;
    return num;
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

void Server::set_commit_idx(std::size_t idx)
{
    assert(_me.commit_idx <= idx);
    assert(idx <= get_current_idx());
    _me.commit_idx = idx;
}

void Server::set_state(raft_state_e state)
{
    /* if became the leader, then update the current leader entry */
    if (state == raft_state_e::LEADER)
        _me.current_leader = _me.node;
    _me.state = state;
}

bmcl::Option<Node&> Server::get_node(bmcl::Option<node_id> id)
{
    if (id.isNone()) return bmcl::None;
    return get_node(id.unwrap());
}

bmcl::Option<Node&> Server::get_node(node_id nodeid)
{
    for (Node& i : _me.nodes)
    {
        if (nodeid == i.get_id())
            return i;
    }
    return bmcl::None;
}

const Node& Server::get_my_node()
{
    const auto& n = get_node(_me.node);
    assert(n.isSome());
    return n.unwrap();
}

bmcl::Option<std::size_t> Server::get_last_log_term() const
{
    std::size_t current_idx = get_current_idx();
    if (0 == current_idx)
        return bmcl::None;

    bmcl::Option<const raft_entry_t&> ety = get_entry_from_idx(current_idx);
    if (ety.isNone())
        return bmcl::None;

    return ety.unwrap().term;
}

}