/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#pragma once
#include <chrono>
#include <bmcl/Option.h>
#include <bmcl/Result.h>
#include <bmcl/ArrayView.h>
#include "raft/Types.h"
#include "raft/Committer.h"
#include "raft/Node.h"
#include "raft/Timer.h"


namespace raft
{

enum class State : uint8_t
{
    Follower,
    PreCandidate,
    Candidate,
    Leader,
    Shutdown,
};
const char* to_string(State s);

class Server
{
    friend class Logger;
public:
    explicit Server(NodeId id, bool isnewCluster, const Applier& applyer, IStorage* storage, ISender* sender = nullptr, IEventHandler* events = nullptr); //create new or join existing cluster ()
    explicit Server(NodeId id, bmcl::ArrayView<NodeId> members, const Applier& applyer, IStorage* storage, ISender* sender = nullptr, IEventHandler* events = nullptr); //create new cluster with initial set of members, which includes id
    explicit Server(NodeId id, std::initializer_list<NodeId> members, const Applier& applyer, IStorage* storage, ISender* sender = nullptr, IEventHandler* events = nullptr); //create new cluster with initial set of members, which includes id

    inline void set_sender(ISender* sender) {_sender = sender; }
    inline void set_applier(const Applier& applier) { _applier = applier; }
    inline void set_event_handler(IEventHandler* events) { _events = events; if (!_events) _events = &_defaultEventsHandler; }

    inline bmcl::Option<NodeId> get_current_leader() const { return _current_leader; }
    inline TermId get_current_term() const { return _current_term; }
    inline bmcl::Option<NodeId> get_voted_for() const { return _voted_for; }
    inline bool is_already_voted() const { return _voted_for.isSome(); }

    inline bool is_follower() const { return get_state() == State::Follower; }
    inline bool is_leader() const { return get_state() == State::Leader; }
    inline bool is_candidate() const { return get_state() == State::Candidate; }
    inline bool is_precandidate() const { return get_state() == State::PreCandidate; }
    inline bool is_shutdown() const { return get_state() == State::Shutdown; }
    inline State get_state() const { return _state; }

    const Nodes& nodes() const { return _nodes; }
    const Committer& committer() const { return _committer; }
    Timer& timer() { return _timer; }
    const Timer& timer() const { return _timer; }
    const IStorage* storage() const { return _storage; }

    bmcl::Option<Error> tick(Time elapsed = Time(0), Index max_count = Index(-1));
    bmcl::Option<Error> apply_all(Index max_count = Index(-1));

    bmcl::Result<MsgAppendEntriesRep, Error> accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgAppendEntriesRep& r);
    bmcl::Result<MsgVoteRep, Error> accept_req(NodeId nodeid, const MsgVoteReq& vr);
    bmcl::Option<Error> accept_rep(NodeId nodeid, const MsgVoteRep& r);

    bmcl::Result<MsgAddEntryRep, Error> add_entry(EntryId id, const UserData& data);
    bmcl::Result<MsgAddEntryRep, Error> add_node(EntryId id, NodeId node);
    bmcl::Result<MsgAddEntryRep, Error> remove_node(EntryId id, NodeId node);
    bmcl::Option<Error> start_election();

    bmcl::Option<Error> send_appendentries(NodeId node);
    bmcl::Option<Error> send_smth_for(NodeId node, ISender* sender);

    void sync_log_and_nodes();

private:
    bmcl::Result<MsgAddEntryRep, Error> accept_entry(const Entry& ety);
    bmcl::Option<Error> set_current_term(TermId term);
    bmcl::Option<Error> vote_for_nodeid(NodeId nodeid);
    void become_follower();
    void become_candidate();
    void become_precandidate();
    void become_leader();
    void set_state(State state);

    bool should_grant_vote(const MsgVoteReq& vr) const;
    MsgAppendEntriesRep prepare_response(NodeId nodeid, bool success, Index index);
    MsgVoteRep prepare_requestvote_response_t(NodeId candidate, ReqVoteState vote);
    bmcl::Option<Error> send_appendentries(Node& node, ISender* sender);
    bmcl::Option<Error> send_reqvote(Node& node, ISender* sender);

    void entry_pop(const Entry& ety);
    bmcl::Option<Error> entry_push(const Entry& ety, bool needVoteChecks);
    bmcl::Option<Error> entry_apply_one();

    bmcl::Option<NodeId>    _voted_for;      /**< The candidate the server voted for in its current term, or Nil if it hasn't voted for any.  */
    bmcl::Option<NodeId>    _current_leader; /**< what this node thinks is the node ID of the current leader, or -1 if there isn't a known current leader. */
    TermId                  _current_term;   /**< the server's best guess of what the current term is starts at zero */
    State                   _state;          /**< follower/leader/candidate indicator */
    Index                   _last_cfg_seen;

    Timer     _timer;
    Nodes     _nodes;
    Committer _committer;
    IStorage* _storage;
    ISender*  _sender;
    Applier   _applier;
    IEventHandler* _events;
    IEventHandler _defaultEventsHandler;

};

}