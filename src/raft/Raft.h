/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#pragma once
#include <vector>
#include <chrono>
#include <functional>
#include <bmcl/Option.h>
#include <bmcl/Result.h>
#include "Types.h"
#include "Log.h"
#include "Node.h"

class Raft
{
    struct raft_server_private_t
    {
        /* Persistent state: */
        int current_term;                       /**< the server's best guess of what the current term is starts at zero */
        bmcl::Option<raft_node_id> voted_for;   /**< The candidate the server voted for in its current term, or Nil if it hasn't voted for any.  */
        RaftLog log;                            /**< the log which is replicated */

        /* Volatile state: */
        std::size_t commit_idx;                                 /**< idx of highest log entry known to be committed */
        std::size_t last_applied_idx;                           /**< idx of highest log entry applied to state machine */
        raft_state_e state;                                     /**< follower/leader/candidate indicator */
        std::chrono::milliseconds timeout_elapsed;              /**< amount of time left till timeout */
        std::vector<RaftNode> nodes;
        std::chrono::milliseconds election_timeout;
        std::chrono::milliseconds request_timeout;
        bmcl::Option<raft_node_id> current_leader;              /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */
        raft_cbs_t cb;                                          /**< callbacks */
        bmcl::Option<raft_node_id> node;                        /**< my node ID */
        bmcl::Option<std::size_t> voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
        raft_node_status connected;                             /**< our membership with the cluster is confirmed (ie. configuration log was committed) */
    };

    friend class RaftLog;
public:
    explicit Raft(raft_node_id id, bool is_voting, const raft_cbs_t& funcs = raft_cbs_t{});
    void raft_set_callbacks(const raft_cbs_t& funcs);
    bmcl::Option<RaftNode&> raft_add_node(raft_node_id id);
    bmcl::Option<RaftNode&> raft_add_non_voting_node(raft_node_id id);
    void raft_remove_node(raft_node_id id);
    void raft_remove_node(const bmcl::Option<RaftNode&>& node);
    void raft_set_election_timeout(std::chrono::milliseconds msec);
    void raft_set_request_timeout(std::chrono::milliseconds msec);
    bmcl::Option<RaftError> raft_periodic(std::chrono::milliseconds msec_elapsed);

    bmcl::Result<msg_appendentries_response_t, RaftError> raft_recv_appendentries(bmcl::Option<raft_node_id> nodeid, const msg_appendentries_t& ae);
    bmcl::Option<RaftError> raft_recv_appendentries_response(bmcl::Option<raft_node_id> nodeid, const msg_appendentries_response_t& r);
    bmcl::Result<msg_requestvote_response_t, RaftError> raft_recv_requestvote(bmcl::Option<raft_node_id> nodeid, const msg_requestvote_t& vr);
    bmcl::Option<RaftError> raft_recv_requestvote_response(bmcl::Option<raft_node_id> nodeid, const msg_requestvote_response_t& r);
    bmcl::Result<msg_entry_response_t, RaftError> raft_recv_entry(const msg_entry_t& ety);

    bool raft_is_my_node(raft_node_id id) const;
    bmcl::Option<raft_node_id> raft_get_my_nodeid() const;
    bmcl::Option<RaftNode&> raft_get_my_node();

    bmcl::Option<raft_node_id> raft_get_current_leader() const;
    bmcl::Option<RaftNode&> raft_get_current_leader_node();

    bmcl::Option<RaftNode&> raft_get_node(raft_node_id id);
    bmcl::Option<RaftNode&> raft_get_node(bmcl::Option<raft_node_id> id);

    std::chrono::milliseconds raft_get_election_timeout() const;
    std::size_t raft_get_num_nodes();
    std::size_t raft_get_num_voting_nodes();
    std::size_t raft_get_log_count();
    int raft_get_current_term() const;
    std::size_t raft_get_current_idx() const;
    std::size_t raft_get_commit_idx() const;
    bool raft_is_follower() const;
    bool raft_is_leader() const;
    bool raft_is_candidate() const;
    std::chrono::milliseconds raft_get_timeout_elapsed() const;
    std::chrono::milliseconds raft_get_request_timeout() const;
    std::size_t raft_get_last_applied_idx() const;
    bmcl::Option<const raft_entry_t&> raft_get_entry_from_idx(std::size_t idx) const;
    bmcl::Option<RaftNode&> raft_get_node_from_idx(std::size_t idx);
    std::size_t raft_get_nvotes_for_me();
    bmcl::Option<raft_node_id> raft_get_voted_for();
    void raft_vote_for_nodeid(bmcl::Option<raft_node_id> nodeid);
    void raft_set_current_term(const int term);
    void raft_set_commit_idx(std::size_t commit_idx);
    void raft_set_last_applied_idx(std::size_t idx);
    bmcl::Option<RaftError> raft_append_entry(const raft_entry_t& ety);
    int raft_msg_entry_response_committed(const msg_entry_response_t& r) const;
    raft_state_e raft_get_state() const;
    bmcl::Option<int> raft_get_last_log_term() const;
    bmcl::Option<RaftError> raft_apply_all();
    void raft_become_leader();
    static bool raft_entry_is_voting_cfg_change(const raft_entry_t& ety);
    static bool raft_entry_is_cfg_change(const raft_entry_t& ety);
    bool raft_already_voted() const;
    raft_node_status raft_is_connected() const;

public:

    void raft_election_start();
    void raft_become_candidate();
    void raft_become_follower();
    bmcl::Option<RaftError> raft_send_requestvote(const bmcl::Option<raft_node_id>& node);
    bmcl::Option<RaftError> raft_send_requestvote(const RaftNode& node);
    bmcl::Option<RaftError> raft_send_appendentries(const bmcl::Option<raft_node_id>& node);
    bmcl::Option<RaftError> raft_send_appendentries(const RaftNode& node);
    bmcl::Option<RaftError> raft_send_appendentries_all();
    bmcl::Option<RaftError> raft_apply_entry();
    void raft_set_state(raft_state_e state);

    static bool raft_votes_is_majority(std::size_t nnodes, std::size_t nvotes);
    void raft_pop_log(const raft_entry_t& ety, const std::size_t idx);
    void raft_offer_log(const raft_entry_t& ety, const std::size_t idx);
    void raft_delete_entry_from_idx(std::size_t idx);
    bool raft_voting_change_is_in_progress() const;
    bmcl::Option<const raft_entry_t*> raft_get_entries_from_idx(std::size_t idx, std::size_t* n_etys) const;
    const raft_cbs_t& get_callbacks() const { return _me.cb; }

    void __log(const bmcl::Option<RaftNode&> node, const char *fmt, ...);
    void __log(const bmcl::Option<const RaftNode&> node, const char *fmt, ...) const;

    raft_server_private_t _me;
};
