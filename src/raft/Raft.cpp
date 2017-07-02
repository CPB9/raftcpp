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

void Raft::__log(const bmcl::Option<RaftNode&> node, const char *fmt, ...)
{
    if (!_me.cb.log)
        return;

    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    _me.cb.log(this, node, buf);
}

void Raft::__log(const bmcl::Option<const RaftNode&> node, const char *fmt, ...) const
{
    if (!_me.cb.log)
        return;

    char buf[1024];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    _me.cb.log(this, node, buf);
}

Raft::Raft()
{
    _me.cb = { 0 };
    _me.commit_idx = 0;
    _me.last_applied_idx = 0;

    _me.current_term = 0;
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    _me.request_timeout = std::chrono::milliseconds(200);
    _me.election_timeout = std::chrono::milliseconds(1000);
    raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);
}

Raft::Raft(void* user_data, raft_node_id id) : Raft{}
{
    raft_add_node(user_data, id, true);
}

void Raft::raft_set_callbacks(const raft_cbs_t& funcs)
{
    _me.cb = funcs;
}

void Raft::raft_delete_entry_from_idx(std::size_t idx)
{
    assert(_me.commit_idx < idx);
    if (_me.voting_cfg_change_log_idx.isSome() && idx <= _me.voting_cfg_change_log_idx.unwrap())
        _me.voting_cfg_change_log_idx.clear();
    _me.log.log_delete(this, idx);
}

void Raft::raft_election_start()
{
    __log(NULL, "election starting: %d %d, term: %d ci: %d",
          _me.election_timeout.count(), _me.timeout_elapsed.count(), _me.current_term,
          raft_get_current_idx());

    raft_become_candidate();
}

void Raft::raft_become_leader()
{
    __log(NULL, "becoming leader term:%d", raft_get_current_term());

    raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    for (RaftNode& i: _me.nodes)
    {
        if (_me.node == i.raft_node_get_id())
            continue;
        i.raft_node_set_next_idx(raft_get_current_idx() + 1);
        i.raft_node_set_match_idx(0);
        raft_send_appendentries(i);
    }
}

void Raft::raft_become_candidate()
{
    __log(NULL, "becoming candidate");

    raft_set_current_term(raft_get_current_term() + 1);
    for (RaftNode& i : _me.nodes)
        i.raft_node_vote_for_me(false);
    raft_vote_for_nodeid(_me.node);
    _me.current_leader.clear();
    raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    _me.timeout_elapsed = std::chrono::milliseconds(_me.election_timeout.count() - 2 * (rand() % _me.election_timeout.count()));

    for (const RaftNode& i : _me.nodes)
        if (_me.node != i.raft_node_get_id() && i.raft_node_is_voting())
            raft_send_requestvote(i);
}

void Raft::raft_become_follower()
{
    __log(NULL, "becoming follower");
    raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);
}

bmcl::Option<RaftError> Raft::raft_periodic(std::chrono::milliseconds msec_since_last_period)
{
    _me.timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    if (1 == raft_get_num_voting_nodes())
    {
        bmcl::Option<RaftNode&> node = raft_get_my_node();
        if (node.isSome() && node->raft_node_is_voting() && !raft_is_leader())
            raft_become_leader();
    }

    if (_me.state == raft_state_e::RAFT_STATE_LEADER)
    {
        if (_me.request_timeout <= _me.timeout_elapsed)
            raft_send_appendentries_all();
    }
    else if (_me.election_timeout <= _me.timeout_elapsed)
    {
        if (1 < raft_get_num_voting_nodes())
        {
            bmcl::Option<RaftNode&> node = raft_get_my_node();
            if (node.isSome() && node->raft_node_is_voting())
                raft_election_start();
        }
    }

    if (_me.last_applied_idx < _me.commit_idx)
        return raft_apply_entry();

    return bmcl::None;
}

bmcl::Option<const raft_entry_t&> Raft::raft_get_entry_from_idx(std::size_t etyidx) const
{
    return _me.log.log_get_at_idx(etyidx);
}

bool Raft::raft_voting_change_is_in_progress() const
{
    return _me.voting_cfg_change_log_idx.isSome();
}

bmcl::Option<RaftError> Raft::raft_recv_appendentries_response(bmcl::Option<raft_node_id> nodeid, const msg_appendentries_response_t& r)
{
    bmcl::Option<RaftNode&> node = raft_get_node(nodeid);
    __log(node,
          "received appendentries response %s ci:%d rci:%d 1stidx:%d",
          r.success ? "SUCCESS" : "fail",
          raft_get_current_idx(),
          r.current_idx,
          r.first_idx);

    if (node.isNone())
        return RaftError::NodeUnknown;

    /* Stale response -- ignore */
    if (r.current_idx != 0 && r.current_idx <= node->raft_node_get_match_idx())
        return bmcl::None;

    if (!raft_is_leader())
        return RaftError::NotLeader;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (_me.current_term < r.term)
    {
        raft_set_current_term(r.term);
        raft_become_follower();
        return bmcl::None;
    }
    else if (_me.current_term != r.term)
        return bmcl::None;

    if (!r.success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        std::size_t next_idx = node->raft_node_get_next_idx();
        assert(0 < next_idx);
        if (r.current_idx < next_idx - 1)
            node->raft_node_set_next_idx(std::min(r.current_idx + 1, raft_get_current_idx()));
        else
            node->raft_node_set_next_idx(next_idx - 1);

        /* retry */
        raft_send_appendentries(node.unwrap());
        return bmcl::None;
    }

    assert(r.current_idx <= raft_get_current_idx());

    node->raft_node_set_next_idx(r.current_idx + 1);
    node->raft_node_set_match_idx(r.current_idx);

    if (!node->raft_node_is_voting() &&
        !raft_voting_change_is_in_progress() &&
        raft_get_current_idx() <= r.current_idx + 1 &&
        _me.cb.node_has_sufficient_logs &&
        false == node->raft_node_has_sufficient_logs()
        )
    {
        bool e = _me.cb.node_has_sufficient_logs(this, node.unwrap());
        if (e)
            node->raft_node_set_has_sufficient_logs();
    }

    /* Update commit idx */
    std::size_t point = r.current_idx;
    if (point)
    {
        bmcl::Option<const raft_entry_t&> ety = raft_get_entry_from_idx(point);
        assert(ety.isSome());
        if (raft_get_commit_idx() < point && ety.unwrap().term == _me.current_term)
        {
            std::size_t votes = 1;
            for (const RaftNode& i: _me.nodes)
            {
                if (_me.node != i.raft_node_get_id_as_option() &&
                    i.raft_node_is_voting() &&
                    point <= i.raft_node_get_match_idx())
                {
                    votes++;
                }
            }

            if (raft_get_num_voting_nodes() / 2 < votes)
                raft_set_commit_idx(point);
        }
    }

    /* Aggressively send remaining entries */
    if (raft_get_entry_from_idx(node->raft_node_get_next_idx()).isSome())
        raft_send_appendentries(node.unwrap());

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

bmcl::Option<RaftError> Raft::raft_recv_appendentries(bmcl::Option<raft_node_id> nodeid, const msg_appendentries_t& ae, msg_appendentries_response_t *r)
{
    bmcl::Option<RaftNode&> node = raft_get_node(nodeid);

    _me.timeout_elapsed = std::chrono::milliseconds(0);

    if (0 < ae.n_entries)
        __log(node, "recvd appendentries t:%d ci:%d lc:%d pli:%d plt:%d #%d",
              ae.term,
              raft_get_current_idx(),
              ae.leader_commit,
              ae.prev_log_idx,
              ae.prev_log_term,
              ae.n_entries);

    r->term = _me.current_term;

    if (raft_is_candidate() && _me.current_term == ae.term)
    {
        raft_become_follower();
    }
    else if (_me.current_term < ae.term)
    {
        raft_set_current_term(ae.term);
        r->term = ae.term;
        raft_become_follower();
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
        bmcl::Option<const raft_entry_t&> e = raft_get_entry_from_idx(ae.prev_log_idx);
        if (e.isNone())
        {
            __log(node, "AE no log at prev_idx %d", ae.prev_log_idx);
            goto fail_with_current_idx;
        }

        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        if (raft_get_current_idx() < ae.prev_log_idx)
            goto fail_with_current_idx;

        if (e.unwrap().term != ae.prev_log_term)
        {
            __log(node, "AE term doesn't match prev_term (ie. %d vs %d) ci:%d pli:%d",
                e.unwrap().term, ae.prev_log_term, raft_get_current_idx(), ae.prev_log_idx);
            /* Delete all the following log entries because they don't match */
            raft_delete_entry_from_idx(ae.prev_log_idx);
            r->current_idx = ae.prev_log_idx - 1;
            goto fail;
        }
    }

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    if (0 < ae.prev_log_idx && ae.prev_log_idx + 1 < raft_get_current_idx())
    {
        /* Heartbeats shouldn't cause logs to be deleted. Heartbeats might be
        * sent before the leader received the last appendentries response */
        if (ae.n_entries != 0 &&
            /* this is an old out-of-order appendentry message */
            _me.commit_idx < ae.prev_log_idx + 1)
            raft_delete_entry_from_idx(ae.prev_log_idx + 1);
    }

    r->current_idx = ae.prev_log_idx;

    std::size_t i;
    for (i = 0; i < ae.n_entries; i++)
    {
        const raft_entry_t* ety = &ae.entries[i];
        std::size_t ety_index = ae.prev_log_idx + 1 + i;
        bmcl::Option<const raft_entry_t&> existing_ety = raft_get_entry_from_idx(ety_index);
        r->current_idx = ety_index;
        if (existing_ety.isSome() && existing_ety.unwrap().term != ety->term && _me.commit_idx < ety_index)
        {
            raft_delete_entry_from_idx(ety_index);
            break;
        }
        else if (existing_ety.isNone())
            break;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < ae.n_entries; i++)
    {
        bmcl::Option<RaftError> e = raft_append_entry(ae.entries[i]);
        if (e.isSome())
        {
            if (e.unwrap() == RaftError::Shutdown)
            {
                r->success = false;
                r->first_idx = 0;
                return RaftError::Shutdown;
            }
            goto fail_with_current_idx;
        }
        r->current_idx = ae.prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    if (raft_get_commit_idx() < ae.leader_commit)
    {
        std::size_t last_log_idx = std::max<std::size_t>(raft_get_current_idx(), 1);
        raft_set_commit_idx(std::min(last_log_idx, ae.leader_commit));
    }

    /* update current leader because we accepted appendentries from it */
    _me.current_leader = nodeid;

    r->success = true;
    r->first_idx = ae.prev_log_idx + 1;
    return bmcl::None;

fail_with_current_idx:
    r->current_idx = raft_get_current_idx();
fail:
    r->success = false;
    r->first_idx = 0;
    return RaftError::Any;
}

bool Raft::raft_already_voted() const
{
    return _me.voted_for.isSome();
}

static bool __should_grant_vote(Raft* me, const msg_requestvote_t& vr)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!me->raft_get_my_node()->raft_node_is_voting())
        return false;

    if (vr.term < me->raft_get_current_term())
        return false;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (me->raft_already_voted())
        return false;

    /* Below we check if log is more up-to-date... */

    std::size_t current_idx = me->raft_get_current_idx();

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return true;

    bmcl::Option<const raft_entry_t&> e = me->raft_get_entry_from_idx(current_idx);
    assert((current_idx != 0) == e.isSome());
    if (e.isNone())
        return true;


    if (e.unwrap().term < vr.last_log_term)
        return true;

    if (vr.last_log_term == e.unwrap().term && current_idx <= vr.last_log_idx)
        return true;

    return false;
}

bmcl::Option<RaftError> Raft::raft_recv_requestvote(bmcl::Option<raft_node_id> nodeid, const msg_requestvote_t& vr, msg_requestvote_response_t *r)
{
    bmcl::Option<RaftNode&> node = raft_get_node(nodeid);

    if (node.isNone())
        node = raft_get_node(vr.candidate_id);

    if (raft_get_current_term() < vr.term)
    {
        raft_set_current_term(vr.term);
        raft_become_follower();
    }

    if (__should_grant_vote(this, vr))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(raft_is_leader() || raft_is_candidate()));

        raft_vote_for_nodeid(vr.candidate_id);
        r->vote_granted = raft_request_vote::GRANTED;

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
            r->vote_granted = raft_request_vote::UNKNOWN_NODE;
            goto done;
        }
        else
            r->vote_granted = raft_request_vote::NOT_GRANTED;
    }

done:
    __log(node, "node requested vote: %d replying: %s",
          node,
          r->vote_granted == raft_request_vote::GRANTED ? "granted" :
          r->vote_granted == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown");

    r->term = raft_get_current_term();
    return bmcl::None;
}

bool Raft::raft_votes_is_majority(std::size_t num_nodes, std::size_t nvotes)
{
    if (num_nodes < nvotes)
        return false;
    std::size_t half = num_nodes / 2;
    return half + 1 <= nvotes;
}

bmcl::Option<RaftError> Raft::raft_recv_requestvote_response(bmcl::Option<raft_node_id> nodeid, const msg_requestvote_response_t& r)
{
    bmcl::Option<RaftNode&> node = raft_get_node(nodeid);

    __log(node, "node responded to requestvote status: %s",
          r.vote_granted == raft_request_vote::GRANTED ? "granted" :
          r.vote_granted == raft_request_vote::NOT_GRANTED ? "not granted" : "unknown");

    if (!raft_is_candidate())
        return bmcl::None;

    if (raft_get_current_term() < r.term)
    {
        raft_set_current_term(r.term);
        raft_become_follower();
        return bmcl::None;
    }

    if (raft_get_current_term() != r.term)
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
                node->raft_node_vote_for_me(true);
            if (Raft::raft_votes_is_majority(raft_get_num_voting_nodes(), raft_get_nvotes_for_me()))
                raft_become_leader();
            break;

        case raft_request_vote::NOT_GRANTED:
            break;

        case raft_request_vote::UNKNOWN_NODE:
            if (raft_get_my_node()->raft_node_is_voting() && _me.connected == raft_node_status::RAFT_NODE_STATUS_DISCONNECTING)
                return RaftError::Shutdown;
            break;

        default:
            assert(0);
    }

    return bmcl::None;
}

bmcl::Option<RaftError> Raft::raft_recv_entry(const msg_entry_t& e, msg_entry_response_t *r)
{
    /* Only one voting cfg change at a time */
    if (raft_entry_is_voting_cfg_change(e))
        if (raft_voting_change_is_in_progress())
            return RaftError::OneVotiongChangeOnly;

    if (!raft_is_leader())
        return RaftError::NotLeader;

    __log(NULL, "received entry t:%d id: %d idx: %d",
        _me.current_term, e.id, raft_get_current_idx() + 1);

    raft_entry_t ety = e;
    ety.term = _me.current_term;
    raft_append_entry(ety);
    for (const RaftNode& i: _me.nodes)
    {
        if (_me.node == i.raft_node_get_id_as_option() || !i.raft_node_is_voting())
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        std::size_t next_idx = i.raft_node_get_next_idx();
        if (next_idx == raft_get_current_idx())
            raft_send_appendentries(i);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == raft_get_num_voting_nodes())
        raft_set_commit_idx(raft_get_current_idx());

    r->id = e.id;
    r->idx = raft_get_current_idx();
    r->term = _me.current_term;

    if (raft_entry_is_voting_cfg_change(e))
        _me.voting_cfg_change_log_idx = raft_get_current_idx();

    return bmcl::None;
}

bmcl::Option<RaftError> Raft::raft_send_requestvote(const bmcl::Option<raft_node_id>& node)
{
    bmcl::Option<RaftNode&> n = raft_get_node(node);
    if (n.isNone()) return RaftError::NodeUnknown;
    return raft_send_requestvote(n.unwrap());
}

bmcl::Option<RaftError> Raft::raft_send_requestvote(const RaftNode& node)
{
    msg_requestvote_t rv;

    assert(_me.node.isSome());
    assert(node.raft_node_get_id_as_option() != _me.node);

    __log(node, "sending requestvote to: %d", node);

    rv.term = _me.current_term;
    rv.last_log_idx = raft_get_current_idx();
    rv.last_log_term = raft_get_last_log_term().unwrapOr(0);
    rv.candidate_id = raft_get_my_nodeid().unwrap();
    assert(_me.cb.send_requestvote);
    return _me.cb.send_requestvote(this, node, rv);
}

bmcl::Option<RaftError> Raft::raft_append_entry(const raft_entry_t& ety)
{
    if (raft_entry_is_voting_cfg_change(ety))
        _me.voting_cfg_change_log_idx = raft_get_current_idx();
    return _me.log.log_append_entry(this, ety);
}

bmcl::Option<RaftError> Raft::raft_apply_entry()
{
    /* Don't apply after the commit_idx */
    if (_me.last_applied_idx == _me.commit_idx)
        return RaftError::Any;

    std::size_t log_idx = _me.last_applied_idx + 1;

    bmcl::Option<const raft_entry_t&> ety = raft_get_entry_from_idx(log_idx);
    if (ety.isNone())
        return RaftError::Any;

    __log(NULL, "applying log: %d, id: %d size: %d",
        _me.last_applied_idx, ety.unwrap().id, ety.unwrap().data.len);

    _me.last_applied_idx++;
    assert(_me.cb.applylog);
    int e = _me.cb.applylog(this, ety.unwrap(), _me.last_applied_idx - 1);
    if (e == RAFT_ERR_SHUTDOWN)
        return RaftError::Shutdown;
    assert(e == 0);

    /* Membership Change: confirm connection with cluster */
    if (raft_logtype_e::RAFT_LOGTYPE_ADD_NODE == ety.unwrap().type)
    {
        raft_node_id node_id = (raft_node_id)_me.cb.log_get_node_id(this, ety.unwrap(), log_idx);
        bmcl::Option<RaftNode&> node = raft_get_node(node_id);
        assert(node.isSome());
        if (node.isSome())
        {
            node->raft_node_set_has_sufficient_logs();
            if (node->raft_node_get_id_as_option() == raft_get_my_nodeid())
                _me.connected = raft_node_status::RAFT_NODE_STATUS_CONNECTED;
        }
    }

    /* voting cfg change is now complete */
    if (_me.voting_cfg_change_log_idx.isSome() && log_idx == _me.voting_cfg_change_log_idx.unwrap())
        _me.voting_cfg_change_log_idx.clear();

    return bmcl::None;
}

bmcl::Option<const raft_entry_t *> Raft::raft_get_entries_from_idx(std::size_t idx, std::size_t* n_etys) const
{
    return _me.log.log_get_from_idx(idx, n_etys);
}

bmcl::Option<RaftError> Raft::raft_send_appendentries(const bmcl::Option<raft_node_id>& node)
{
    bmcl::Option<RaftNode&> n = raft_get_node(node);
    if (n.isNone()) return RaftError::NodeUnknown;
    return raft_send_appendentries(n.unwrap());
}

bmcl::Option<RaftError> Raft::raft_send_appendentries(const RaftNode& node)
{
    assert(node.raft_node_get_id_as_option() != _me.node);

    msg_appendentries_t ae = {0};
    ae.term = _me.current_term;
    ae.leader_commit = raft_get_commit_idx();
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    std::size_t next_idx = node.raft_node_get_next_idx();

    ae.entries = raft_get_entries_from_idx(next_idx, &ae.n_entries).unwrapOr(nullptr);

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        bmcl::Option<const raft_entry_t&> prev_ety = raft_get_entry_from_idx(next_idx - 1);
        ae.prev_log_idx = next_idx - 1;
        if (prev_ety.isSome())
            ae.prev_log_term = prev_ety.unwrap().term;
    }

    __log(node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d",
          raft_get_current_idx(),
          raft_get_commit_idx(),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term);

    assert(_me.cb.send_appendentries);
    return _me.cb.send_appendentries(this, node, ae);
}

bmcl::Option<RaftError> Raft::raft_send_appendentries_all()
{
    _me.timeout_elapsed = std::chrono::milliseconds(0);
    for (const RaftNode& i: _me.nodes)
    {
        if (_me.node != i.raft_node_get_id_as_option())
        {
            bmcl::Option<RaftError> e = raft_send_appendentries(i);
            if (e.isSome())
                return e;
        }
    }
    return bmcl::None;
}

bmcl::Option<RaftNode&> Raft::raft_add_node(void* udata, raft_node_id id, bool is_self)
{
    /* set to voting if node already exists */
    bmcl::Option<RaftNode&> node = raft_get_node(id);
    if (node.isSome())
    {
        if (!node->raft_node_is_voting())
        {
            node->raft_node_set_voting(true);
            return node;
        }
        else
            /* we shouldn't add a node twice */
            return bmcl::None;
    }

    _me.nodes.emplace_back(RaftNode(udata, id));
    if (is_self)
        _me.node = _me.nodes.back().raft_node_get_id();

    return _me.nodes.back();
}

bmcl::Option<RaftNode&> Raft::raft_add_non_voting_node(void* udata, raft_node_id id, bool is_self)
{
    if (raft_get_node(id).isSome())
        return bmcl::None;

    bmcl::Option<RaftNode&> node = raft_add_node(udata, id, is_self);
    if (node.isNone())
        return bmcl::None;

    node->raft_node_set_voting(false);
    return node;
}

void Raft::raft_remove_node(raft_node_id nodeid)
{
    for (std::size_t i = 0; i < _me.nodes.size(); i++)
    {
        if (_me.nodes[i].raft_node_get_id() == nodeid)
        {
            _me.nodes.erase(_me.nodes.begin() + i);
            return;
        }
    }
    assert(false);
}

void Raft::raft_remove_node(const bmcl::Option<RaftNode&>& node)
{
    if (node.isNone())
        return;
    raft_remove_node(node->raft_node_get_id());
}

std::size_t Raft::raft_get_nvotes_for_me()
{
    std::size_t votes = 0;

    for (const RaftNode& i: _me.nodes)
    {
        if (_me.node != i.raft_node_get_id_as_option() && i.raft_node_is_voting())
            if (i.raft_node_has_vote_for_me())
                votes += 1;
    }

    if (_me.voted_for == raft_get_my_nodeid())
        votes += 1;

    return votes;
}

void Raft::raft_vote_for_nodeid(bmcl::Option<raft_node_id> nodeid)
{
    _me.voted_for = nodeid;
    assert(_me.cb.persist_vote);
    int id = -1;
    if (nodeid.isSome()) id = (int)nodeid.unwrap();
    _me.cb.persist_vote(this, -1);
}

int Raft::raft_msg_entry_response_committed(const msg_entry_response_t& r) const
{
    bmcl::Option<const raft_entry_t&> ety = raft_get_entry_from_idx(r.idx);
    if (ety.isNone())
        return 0;

    /* entry from another leader has invalidated this entry message */
    if (r.term != ety.unwrap().term)
        return -1;
    return r.idx <= raft_get_commit_idx();
}

bmcl::Option<RaftError> Raft::raft_apply_all()
{
    while (raft_get_last_applied_idx() < raft_get_commit_idx())
    {
        bmcl::Option<RaftError> e = raft_apply_entry();
        if (e.isSome())
            return e;
    }

    return bmcl::None;
}

bool Raft::raft_entry_is_voting_cfg_change(const raft_entry_t& ety)
{
    return raft_logtype_e::RAFT_LOGTYPE_ADD_NODE == ety.type ||
        raft_logtype_e::RAFT_LOGTYPE_DEMOTE_NODE == ety.type;
}

bool Raft::raft_entry_is_cfg_change(const raft_entry_t& ety)
{
    return (
        raft_logtype_e::RAFT_LOGTYPE_ADD_NODE == ety.type ||
        raft_logtype_e::RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety.type ||
        raft_logtype_e::RAFT_LOGTYPE_DEMOTE_NODE == ety.type ||
        raft_logtype_e::RAFT_LOGTYPE_REMOVE_NODE == ety.type);
}

void Raft::raft_offer_log(const raft_entry_t& ety, const std::size_t idx)
{
    if (!raft_entry_is_cfg_change(ety))
        return;

    raft_node_id node_id = (raft_node_id)_me.cb.log_get_node_id(this, ety, idx);
    bmcl::Option<RaftNode&> node = raft_get_node(node_id);
    bool is_self = raft_is_my_node(node_id);

    switch (ety.type)
    {
    case raft_logtype_e::RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                bmcl::Option<RaftNode&> node = raft_add_non_voting_node(NULL, node_id, is_self);
                assert(node.isSome());
            }
            break;

        case raft_logtype_e::RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node(NULL, node_id, is_self);
            assert(node.isSome());
            assert(node->raft_node_is_voting());
            break;

        case raft_logtype_e::RAFT_LOGTYPE_DEMOTE_NODE:
            node->raft_node_set_voting(false);
            break;

        case raft_logtype_e::RAFT_LOGTYPE_REMOVE_NODE:
            if (node.isSome())
                raft_remove_node(node->raft_node_get_id());
            break;

        default:
            assert(0);
    }
}

void Raft::raft_pop_log(const raft_entry_t& ety, const std::size_t idx)
{
    if (!raft_entry_is_cfg_change(ety))
        return;

    raft_node_id node_id = (raft_node_id)_me.cb.log_get_node_id(this, ety, idx);

    switch (ety.type)
    {
        case raft_logtype_e::RAFT_LOGTYPE_DEMOTE_NODE:
            {
                bmcl::Option<RaftNode&> node = raft_get_node(node_id);
                node->raft_node_set_voting(true);
            }
            break;

        case raft_logtype_e::RAFT_LOGTYPE_REMOVE_NODE:
            {
                bool is_self = raft_is_my_node(node_id);
                bmcl::Option<RaftNode&> node = raft_add_non_voting_node(NULL, node_id, is_self);
                assert(node.isSome());
            }
            break;

        case raft_logtype_e::RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            {
                bool is_self = raft_is_my_node(node_id);
                raft_remove_node(node_id);
                assert(!is_self);
            }
            break;

        case raft_logtype_e::RAFT_LOGTYPE_ADD_NODE:
            {
                bmcl::Option<RaftNode&> node = raft_get_node(node_id);
                node->raft_node_set_voting(false);
            }
            break;

        default:
            assert(false);
            break;
    }
}




//properties


void Raft::raft_set_election_timeout(std::chrono::milliseconds millisec)
{
    _me.election_timeout = millisec;
}

void Raft::raft_set_request_timeout(std::chrono::milliseconds millisec)
{
    _me.request_timeout = millisec;
}

bool Raft::raft_is_my_node(raft_node_id id) const
{
    return (_me.node.isSome() && _me.node.unwrap() == id);
}

bmcl::Option<raft_node_id> Raft::raft_get_my_nodeid() const
{
    return _me.node;
}

std::chrono::milliseconds Raft::raft_get_election_timeout() const
{
    return _me.election_timeout;
}

std::chrono::milliseconds Raft::raft_get_request_timeout() const
{
    return _me.request_timeout;
}

std::size_t Raft::raft_get_num_nodes()
{
    return _me.nodes.size();
}

std::size_t Raft::raft_get_num_voting_nodes()
{
    std::size_t num = 0;
    for (const RaftNode& i: _me.nodes)
        if (i.raft_node_is_voting())
            num++;
    return num;
}

std::chrono::milliseconds Raft::raft_get_timeout_elapsed() const
{
    return _me.timeout_elapsed;
}

std::size_t Raft::raft_get_log_count()
{
    return _me.log.log_count();
}

bmcl::Option<raft_node_id> Raft::raft_get_voted_for()
{
    return _me.voted_for;
}

void Raft::raft_set_current_term(const int term)
{
    if (_me.current_term < term)
    {
        _me.current_term = term;
        _me.voted_for.clear();
        assert(_me.cb.persist_term);
        _me.cb.persist_term(this, term);
    }
}

int Raft::raft_get_current_term() const
{
    return _me.current_term;
}

std::size_t Raft::raft_get_current_idx() const
{
    return _me.log.log_get_current_idx();
}

void Raft::raft_set_commit_idx(std::size_t idx)
{
    assert(_me.commit_idx <= idx);
    assert(idx <= raft_get_current_idx());
    _me.commit_idx = idx;
}

void Raft::raft_set_last_applied_idx(std::size_t idx)
{
    _me.last_applied_idx = idx;
}

std::size_t Raft::raft_get_last_applied_idx() const
{
    return _me.last_applied_idx;
}

std::size_t Raft::raft_get_commit_idx() const
{
    return _me.commit_idx;
}

void Raft::raft_set_state(raft_state_e state)
{
    /* if became the leader, then update the current leader entry */
    if (state == raft_state_e::RAFT_STATE_LEADER)
        _me.current_leader = _me.node;
    _me.state = state;
}

raft_state_e Raft::raft_get_state() const
{
    return _me.state;
}

bmcl::Option<RaftNode&> Raft::raft_get_node(bmcl::Option<raft_node_id> id)
{
    if (id.isNone()) return bmcl::None;
    return raft_get_node(id.unwrap());
}

bmcl::Option<RaftNode&> Raft::raft_get_node(raft_node_id nodeid)
{
    for (RaftNode& i : _me.nodes)
    {
        if (nodeid == i.raft_node_get_id())
            return i;
    }
    return bmcl::None;
}

bmcl::Option<RaftNode&> Raft::raft_get_my_node()
{
    return raft_get_node(raft_get_my_nodeid());
}

bmcl::Option<RaftNode&> Raft::raft_get_node_from_idx(std::size_t idx)
{
    if (idx >= _me.nodes.size())
        return bmcl::None;
    return _me.nodes[idx];
}

bmcl::Option<raft_node_id> Raft::raft_get_current_leader() const
{
    return _me.current_leader;
}

bmcl::Option<RaftNode&> Raft::raft_get_current_leader_node()
{
    return raft_get_node(_me.current_leader);
}

bool Raft::raft_is_follower() const
{
    return raft_get_state() == raft_state_e::RAFT_STATE_FOLLOWER;
}

bool Raft::raft_is_leader() const
{
    return raft_get_state() == raft_state_e::RAFT_STATE_LEADER;
}

bool Raft::raft_is_candidate() const
{
    return raft_get_state() == raft_state_e::RAFT_STATE_CANDIDATE;
}

bmcl::Option<int> Raft::raft_get_last_log_term() const
{
    std::size_t current_idx = raft_get_current_idx();
    if (0 == current_idx)
        return bmcl::None;

    bmcl::Option<const raft_entry_t&> ety = raft_get_entry_from_idx(current_idx);
    if (ety.isNone())
        return bmcl::None;

    return ety.unwrap().term;
}

raft_node_status Raft::raft_is_connected() const
{
    return _me.connected;
}



