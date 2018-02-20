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
#include <bmcl/ArrayView.h>
#include "Types.h"
#include "Log.h"
#include "Node.h"
#include "Timer.h"


namespace raft
{

class Server
{
    struct server_private_t
    {
        bmcl::Option<NodeId>    voted_for;      /**< The candidate the server voted for in its current term, or Nil if it hasn't voted for any.  */
        bmcl::Option<NodeId>    current_leader; /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */
        TermId                  current_term;   /**< the server's best guess of what the current term is starts at zero */
        State                   state;          /**< follower/leader/candidate indicator */
    };

    friend class Logger;
public:
    explicit Server(NodeId id, bool isnewCluster, ISender* sender = nullptr, ISaver* saver = nullptr); //create new or join existing cluster ()
    explicit Server(NodeId id, bmcl::ArrayView<NodeId> members, ISender* sender = nullptr, ISaver* saver = nullptr); //create new cluster with initial set of members, which includes id
    explicit Server(NodeId id, std::initializer_list<NodeId> members, ISender* sender = nullptr, ISaver* saver = nullptr); //create new cluster with initial set of members, which includes id

    inline void set_sender(ISender* sender) {_sender = sender; }
    inline void set_saver(ISaver* saver) { _saver = saver; }
    inline const ISaver* get_saver() const { return _saver; }

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
    Timer& timer() { return _timer; }
    const Timer& timer() const { return _timer; }

    bmcl::Option<Error> tick(std::chrono::milliseconds elapsed = std::chrono::milliseconds(0));

    bmcl::Result<MsgAppendEntriesRep, Error> accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgAppendEntriesRep& r);
    MsgVoteRep accept_req(NodeId nodeid, const MsgVoteReq& vr);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgVoteRep& r);

    bmcl::Result<MsgAddEntryRep, Error> add_entry(EntryId id, const EntryData& data);
    bmcl::Result<MsgAddEntryRep, Error> add_node(EntryId id, NodeId node);
    bmcl::Result<MsgAddEntryRep, Error> remove_node(EntryId id, NodeId node);

    bmcl::Option<Error> send_appendentries(NodeId node);
    bmcl::Option<Error> send_smth_for(NodeId node, ISender* sender);

    void sync_log_and_nodes();

private:
    bmcl::Result<MsgAddEntryRep, Error> accept_entry(const Entry& ety);
    bmcl::Option<Error> set_current_term(TermId term);
    bmcl::Option<Error> vote_for_nodeid(NodeId nodeid);
    void become_follower();
    void become_candidate();
    void become_leader();
    void set_state(State state);
    bmcl::Option<Error> send_appendentries(Node& node, ISender* sender);
    bmcl::Option<Error> send_reqvote(Node& node, ISender* sender);
    void pop_log(const Entry& ety, Index idx);
    bmcl::Option<Error> entry_append(const Entry& ety, bool needVoteChecks);
    void __log(const char *fmt, ...) const;
    MsgVoteRep prepare_requestvote_response_t(NodeId candidate, ReqVoteState vote);
    bool should_grant_vote(const MsgVoteReq& vr) const;

    Timer _timer;
    Nodes _nodes;
    LogCommitter _log;
    server_private_t _me;
    ISender* _sender;
    ISaver* _saver;
};

}