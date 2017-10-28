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

namespace raft
{

class Server
{
    struct server_private_t
    {
        /* Persistent state: */
        std::size_t  current_term;              /**< the server's best guess of what the current term is starts at zero */
        bmcl::Option<node_id> voted_for;        /**< The candidate the server voted for in its current term, or Nil if it hasn't voted for any.  */
        Logger log;                             /**< the log which is replicated */

        /* Volatile state: */
        std::size_t commit_idx;                                 /**< idx of highest log entry known to be committed */
        std::size_t last_applied_idx;                           /**< idx of highest log entry applied to state machine */
        raft_state_e state;                                     /**< follower/leader/candidate indicator */
        std::chrono::milliseconds timeout_elapsed;              /**< amount of time left till timeout */
        std::vector<Node> nodes;
        std::chrono::milliseconds election_timeout;
        std::chrono::milliseconds request_timeout;
        bmcl::Option<node_id> current_leader;              /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */
        raft_cbs_t cb;                                          /**< callbacks */
        node_id node;                                      /**< my node ID */
        bmcl::Option<std::size_t> voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
        node_status connected;                             /**< our membership with the cluster is confirmed (ie. configuration log was committed) */
    };

    friend class Logger;
public:
    explicit Server(node_id id, bool is_voting, const raft_cbs_t& funcs = raft_cbs_t{});
    inline bool is_my_node(node_id id) const { return (_me.node == id); }
    inline node_id get_my_nodeid() const { return _me.node; }
    inline void set_callbacks(const raft_cbs_t& funcs) { _me.cb = funcs; }
    inline const raft_cbs_t& get_callbacks() const { return _me.cb; }
    inline void set_election_timeout(std::chrono::milliseconds msec) {_me.election_timeout = msec;}
    inline void set_request_timeout(std::chrono::milliseconds msec) { _me.request_timeout = msec; }
    inline std::chrono::milliseconds get_timeout_elapsed() const { return _me.timeout_elapsed; }
    inline std::chrono::milliseconds get_request_timeout() const { return _me.request_timeout; }
    inline std::chrono::milliseconds get_election_timeout() const { return _me.election_timeout; }

    const Node& get_my_node();
    bmcl::Option<Node&> add_node(node_id id);
    bmcl::Option<Node&> add_non_voting_node(node_id id);
    void remove_node(node_id id);
    void remove_node(const bmcl::Option<Node&>& node);
    bmcl::Option<Node&> get_node(node_id id);
    bmcl::Option<Node&> get_node(bmcl::Option<node_id> id);
    std::size_t get_num_nodes() const { return _me.nodes.size(); }

    std::size_t get_num_voting_nodes() const;
    inline std::size_t get_current_term() const { return _me.current_term; }
    void set_current_term(std::size_t term);
    std::size_t get_nvotes_for_me() const;
    inline bmcl::Option<node_id> get_voted_for() const { return _me.voted_for; }
    void vote_for_nodeid(bmcl::Option<node_id> nodeid);
    inline bool is_already_voted() const { return _me.voted_for.isSome(); }
    static bool raft_votes_is_majority(std::size_t num_nodes, std::size_t nvotes);

    bmcl::Option<Error> raft_periodic(std::chrono::milliseconds msec_elapsed);

    bmcl::Result<msg_appendentries_response_t, Error> raft_recv_appendentries(bmcl::Option<node_id> nodeid, const msg_appendentries_t& ae);
    bmcl::Option<Error> raft_recv_appendentries_response(bmcl::Option<node_id> nodeid, const msg_appendentries_response_t& r);
    bmcl::Result<msg_requestvote_response_t, Error> raft_recv_requestvote(bmcl::Option<node_id> nodeid, const msg_requestvote_t& vr);
    bmcl::Option<Error> raft_recv_requestvote_response(bmcl::Option<node_id> nodeid, const msg_requestvote_response_t& r);
    bmcl::Result<msg_entry_response_t, Error> raft_recv_entry(const msg_entry_t& ety);

    inline bmcl::Option<node_id> get_current_leader() const { return _me.current_leader; }
    inline bmcl::Option<Node&> get_current_leader_node() { return get_node(_me.current_leader); }
    inline bool is_follower() const { return get_state() == raft_state_e::FOLLOWER; }
    inline bool is_leader() const { return get_state() == raft_state_e::LEADER; }
    inline bool is_candidate() const { return get_state() == raft_state_e::CANDIDATE; }
    inline raft_state_e get_state() const { return _me.state; }

    inline std::size_t get_log_count() const { return _me.log.log_count(); }
    inline std::size_t get_current_idx() const { return _me.log.log_get_current_idx(); }
    inline bmcl::Option<const raft_entry_t&> get_entry_from_idx(std::size_t etyidx) const { return _me.log.log_get_at_idx(etyidx); }
    inline std::size_t get_commit_idx() const { return _me.commit_idx; }
    inline std::size_t get_last_applied_idx() const { return _me.last_applied_idx; }
    inline void set_last_applied_idx(std::size_t idx) { _me.last_applied_idx = idx; }
    void set_commit_idx(std::size_t commit_idx);
    bmcl::Option<Error> append_entry(const raft_entry_t& ety);
    int msg_entry_response_committed(const msg_entry_response_t& r) const;
    bmcl::Option<std::size_t> get_last_log_term() const;
    bmcl::Option<Error> apply_all();

public:

    void become_leader();
    void become_candidate();
    void become_follower();
    void election_start();
    bmcl::Option<Error> raft_send_requestvote(const bmcl::Option<node_id>& node);
    bmcl::Option<Error> raft_send_requestvote(const Node& node);
    bmcl::Option<Error> raft_send_appendentries(const bmcl::Option<node_id>& node);
    bmcl::Option<Error> raft_send_appendentries(const Node& node);
    bmcl::Option<Error> raft_send_appendentries_all();
    bmcl::Option<Error> raft_apply_entry();
    void set_state(raft_state_e state);

    void pop_log(const raft_entry_t& ety, const std::size_t idx);
    void offer_log(const raft_entry_t& ety, const std::size_t idx);
    void delete_entry_from_idx(std::size_t idx);
    inline bool voting_change_is_in_progress() const { return _me.voting_cfg_change_log_idx.isSome(); }
    bmcl::Option<const raft_entry_t*> get_entries_from_idx(std::size_t idx, std::size_t* n_etys) const;

    void __log(const bmcl::Option<Node&> node, const char *fmt, ...);
    void __log(const bmcl::Option<const Node&> node, const char *fmt, ...) const;

    server_private_t _me;
};

}