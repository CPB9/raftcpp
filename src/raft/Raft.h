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
        TermId  current_term;              /**< the server's best guess of what the current term is starts at zero */
        bmcl::Option<NodeId> voted_for;        /**< The candidate the server voted for in its current term, or Nil if it hasn't voted for any.  */

        /* Volatile state: */
        State state;                                     /**< follower/leader/candidate indicator */
        bmcl::Option<NodeId>   current_leader;                 /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */

        std::chrono::milliseconds timeout_elapsed;              /**< amount of time left till timeout */
        std::chrono::milliseconds request_timeout;
        std::chrono::milliseconds election_timeout;
        std::chrono::milliseconds election_timeout_rand;
        NodeStatus connected;                                  /**< our membership with the cluster is confirmed (ie. configuration log was committed) */
    };

    friend class Logger;
public:
    explicit Server(NodeId id, bool is_voting, ISender* sender = nullptr, ISaver* saver = nullptr);
    inline void set_sender(ISender* sender) {_sender = sender; }
    inline void set_saver(ISaver* saver) { _saver = saver; }
    inline const ISaver* get_saver() const { return _saver; }
    inline void set_election_timeout(std::chrono::milliseconds msec) {_me.election_timeout = msec;}
    inline void set_request_timeout(std::chrono::milliseconds msec) { _me.request_timeout = msec; }
    inline std::chrono::milliseconds get_timeout_elapsed() const { return _me.timeout_elapsed; }
    inline std::chrono::milliseconds get_request_timeout() const { return _me.request_timeout; }
    inline std::chrono::milliseconds get_election_timeout() const { return _me.election_timeout; }
    inline std::chrono::milliseconds get_max_election_timeout() const { return std::chrono::milliseconds(2 * get_election_timeout().count()); }

    inline bmcl::Option<NodeId> get_current_leader() const { return _me.current_leader; }
    inline TermId get_current_term() const { return _me.current_term; }
    inline bmcl::Option<NodeId> get_voted_for() const { return _me.voted_for; }
    inline bool is_already_voted() const { return _me.voted_for.isSome(); }
    inline bool is_follower() const { return get_state() == State::Follower; }
    inline bool is_leader() const { return get_state() == State::Leader; }
    inline bool is_candidate() const { return get_state() == State::Candidate; }
    inline State get_state() const { return _me.state; }

    const Nodes& nodes() const { return _nodes; }
    Nodes& nodes() { return _nodes; }
    const LogCommitter& log() const { return _log; }
    LogCommitter& log() { return _log; }

    bmcl::Option<Error> raft_periodic(std::chrono::milliseconds msec_elapsed);

    bmcl::Result<MsgAppendEntriesRep, Error> accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgAppendEntriesRep& r);
    MsgVoteRep accept_req(NodeId nodeid, const MsgVoteReq& vr);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgVoteRep& r);
    bmcl::Result<MsgAddEntryRep, Error> accept_entry(const MsgAddEntryReq& ety);

    bmcl::Option<Error> send_appendentries(NodeId node);
    void send_appendentries_to_all();

private:
    void set_current_term(TermId term);
    void vote_for_nodeid(NodeId nodeid);
    void become_follower();
    void become_candidate();
    void become_leader();
    void set_state(State state);
    bmcl::Option<Error> send_appendentries(const Node& node);
    void entry_apply_node_add(const LogEntry& ety, NodeId id);
    void pop_log(const LogEntry& ety, Index idx);
    bmcl::Option<Error> entry_append(const LogEntry& ety);
    void entry_append_impl(const LogEntry& ety, Index idx);
    void __log(NodeId node, const char *fmt, ...) const;
    MsgVoteRep prepare_requestvote_response_t(NodeId candidate, ReqVoteState vote);
    void randomize_election_timeout();

    Nodes _nodes;
    LogCommitter _log;
    server_private_t _me;
    ISender* _sender;
    ISaver* _saver;
};

}