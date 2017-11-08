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

        /* Volatile state: */
        raft_state_e state;                                     /**< follower/leader/candidate indicator */
        bmcl::Option<node_id>   current_leader;                 /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */

        std::chrono::milliseconds timeout_elapsed;              /**< amount of time left till timeout */
        std::chrono::milliseconds election_timeout;
        std::chrono::milliseconds request_timeout;
        raft_cbs_t cb;                                          /**< callbacks */
        node_status connected;                                  /**< our membership with the cluster is confirmed (ie. configuration log was committed) */
    };

    friend class Logger;
public:
    explicit Server(node_id id, bool is_voting, const raft_cbs_t& funcs = raft_cbs_t{});
    inline void set_callbacks(const raft_cbs_t& funcs) { _me.cb = funcs; }
    inline const raft_cbs_t& get_callbacks() const { return _me.cb; }
    inline void set_election_timeout(std::chrono::milliseconds msec) {_me.election_timeout = msec;}
    inline void set_request_timeout(std::chrono::milliseconds msec) { _me.request_timeout = msec; }
    inline std::chrono::milliseconds get_timeout_elapsed() const { return _me.timeout_elapsed; }
    inline std::chrono::milliseconds get_request_timeout() const { return _me.request_timeout; }
    inline std::chrono::milliseconds get_election_timeout() const { return _me.election_timeout; }

    const Nodes& nodes() const { return _nodes; }
    Nodes& nodes() { return _nodes; }
    const LogCommitter& log() const { return _log; }
    LogCommitter& log() { return _log; }

    void set_current_term(std::size_t term);
    void vote_for_nodeid(node_id nodeid);

    bmcl::Option<Error> entry_append(const raft_entry_t& ety);
    raft_entry_state_e entry_get_state(const msg_entry_response_t& r) const;

    bmcl::Option<Error> raft_periodic(std::chrono::milliseconds msec_elapsed);

    bmcl::Result<msg_appendentries_response_t, Error> accept_appendentries(node_id nodeid, const msg_appendentries_t& ae);
    bmcl::Option<Error> accept_appendentries_response(node_id nodeid, const msg_appendentries_response_t& r);
    msg_requestvote_response_t accept_requestvote(node_id nodeid, const msg_requestvote_t& vr);
    bmcl::Option<Error> accept_requestvote_response(node_id nodeid, const msg_requestvote_response_t& r);
    bmcl::Result<msg_entry_response_t, Error> accept_entry(const msg_entry_t& ety);

    inline bmcl::Option<node_id> get_current_leader() const { return _me.current_leader; }
    inline bmcl::Option<Node&> get_current_leader_node() { return _nodes.get_node(_me.current_leader); }
    inline std::size_t get_current_term() const { return _me.current_term; }
    inline bmcl::Option<node_id> get_voted_for() const { return _me.voted_for; }
    inline bool is_already_voted() const { return _me.voted_for.isSome(); }
    inline bool is_follower() const { return get_state() == raft_state_e::FOLLOWER; }
    inline bool is_leader() const { return get_state() == raft_state_e::LEADER; }
    inline bool is_candidate() const { return get_state() == raft_state_e::CANDIDATE; }
    inline raft_state_e get_state() const { return _me.state; }


public:

    void become_leader();
    void become_candidate();
    void become_follower();
    void election_start();
    bmcl::Option<Error> send_appendentries(const bmcl::Option<node_id>& node);
    bmcl::Option<Error> send_appendentries(const Node& node);
    void send_appendentries_all();
    void set_state(raft_state_e state);

    void pop_log(const raft_entry_t& ety, const std::size_t idx);
    void entry_apply_node_add(const raft_entry_t& ety, node_id id);

private:
    void entry_append_impl(const raft_entry_t& ety, const std::size_t idx);
    void __log(const bmcl::Option<node_id&> node, const char *fmt, ...);
    void __log(const bmcl::Option<const node_id&> node, const char *fmt, ...) const;
    msg_requestvote_response_t prepare_requestvote_response_t(node_id candidate, raft_request_vote vote);

    Nodes _nodes;
    LogCommitter _log;
    server_private_t _me;
};

}