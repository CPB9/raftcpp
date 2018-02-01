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
#include <bmcl/Option.h>
#include "Ids.h"
#include "Entry.h"


namespace raft
{

enum class Error : uint8_t
{
    Shutdown,
    NotLeader,
    OneVotingChangeOnly,
    NodeUnknown,
    NothingToApply,
    NothingToSend,
    CantSendToMyself,
    NotCandidate,
    CantSend,
};

enum class ReqVoteState : uint8_t
{
    UnknownNode = 0,
    NotGranted = 1,
    Granted = 2,
};

inline const char* to_string(ReqVoteState vote)
{
    switch (vote)
    {
    case ReqVoteState::Granted: return "granted";
    case ReqVoteState::NotGranted: return "not granted";
    case ReqVoteState::UnknownNode: return "unknown node";
    }
    return "unknown";
}

enum class State : uint8_t
{
    Follower,
    Candidate,
    Leader
} ;

inline const char* to_string(State s)
{
    switch (s)
    {
    case State::Follower: return "follower";
    case State::Candidate: return "candidate";
    case State::Leader: return "leader";
    }
    return "unknown";
}

enum class NodeStatus : uint8_t
{
    Disconnected,
    Connected,
    Connecting,
    Disconnecting,
};

inline const char* to_string(NodeStatus s)
{
    switch (s)
    {
    case raft::NodeStatus::Disconnected: return "disconnected";
    case raft::NodeStatus::Connected: return "connected";
    case raft::NodeStatus::Connecting: return "connecting";
    case raft::NodeStatus::Disconnecting: return "disconnecting";
    }
    return "unknown";
}

/** Message sent from client to server.
 * The client sends this message to a server with the intention of having it
 * applied to the FSM. */
using MsgAddEntryReq = Entry;

/** Entry message response.
 * Indicates to client if entry was committed or not. */
struct MsgAddEntryRep
{
    MsgAddEntryRep(TermId term, EntryId id, Index idx) : term(term), id(id), idx(idx){}
    TermId  term;   /**< the entry's term */
    EntryId id;     /**< the entry's unique ID */
    Index   idx;    /**< the entry's index */
};

/** Vote request message.
 * Sent to nodes when a server wants to become leader.
 * This message could force a leader/candidate to become a follower. */
struct MsgVoteReq
{
    MsgVoteReq(TermId term, Index last_log_idx, TermId last_log_term)
        :term(term), last_log_idx(last_log_idx), last_log_term(last_log_term)
    {
    }
    TermId term;               /**< currentTerm, to force other leader/candidate to step down */
    Index  last_log_idx;       /**< index of candidate's last log entry */
    TermId last_log_term;      /**< term of candidate's last log entry */
};

/** Vote request response message.
 * Indicates if node has accepted the server's vote request. */
struct MsgVoteRep
{
    MsgVoteRep(TermId term, ReqVoteState vote) : term(term), vote_granted(vote) {}
    TermId term;                   /**< currentTerm, for candidate to update itself */
    ReqVoteState vote_granted;     /**< true means candidate received vote */
};

/** Appendentries message.
 * This message is used to tell nodes if it's safe to apply entries to the FSM.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
struct MsgAppendEntriesReq
{
    MsgAppendEntriesReq(TermId term) : term(term), prev_log_idx(0), prev_log_term(TermId(0)), leader_commit(0), n_entries(0), entries(nullptr) {}
    MsgAppendEntriesReq(TermId term, Index prev_log_idx, TermId prev_log_term, Index leader_commit, Index n_entries = 0, const MsgAddEntryReq* entries = nullptr)
        : term(term), prev_log_idx(prev_log_idx), prev_log_term(prev_log_term), leader_commit(leader_commit), n_entries(n_entries), entries(entries) {}
    TermId  term;           /**< currentTerm, to force other leader/candidate to step down */
    Index   prev_log_idx;   /**< the index of the log just before the newest entry for the node who receives this message */
    TermId  prev_log_term;  /**< the term of the log just before the newest entry for the node who receives this message */
    Index   leader_commit;  /**< the index of the entry that has been appended to the majority of the cluster. Entries up to this index will be applied to the FSM */
    Index   n_entries;      /**< number of entries within this message */
    const MsgAddEntryReq* entries; /**< array of entries within this message */
};

/** Appendentries response message.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
struct MsgAppendEntriesRep
{
    MsgAppendEntriesRep(TermId term, bool success, Index current_idx, Index first_idx)
        : term(term), success(success), current_idx(current_idx), first_idx(first_idx) {}
    TermId term;           /**< currentTerm, to force other leader/candidate to step down */
    bool success;               /**< true if follower contained entry matching prevLogidx and prevLogTerm */

    /* Non-Raft fields follow: */
    /* Having the following fields allows us to do less book keeping in regards to full fledged RPC */

    Index current_idx;    /**< This is the highest log IDX we've received and appended to our log */
    Index first_idx;      /**< The first idx that we received within the appendentries message */
} ;

class ISender
{
public:
    virtual ~ISender();

    /** Callback for sending request vote messages to all cluster's members */
    virtual bmcl::Option<Error> request_vote(const NodeId& node, const MsgVoteReq& msg) = 0;

    /** Callback for sending appendentries messages */
    virtual bmcl::Option<Error> append_entries(const NodeId& node, const MsgAppendEntriesReq& msg) = 0;
};

class ISaver
{
public:
    virtual ~ISaver();

    /** Callback for finite state machine application
    * Return 0 on success.
    * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    virtual bmcl::Option<Error> apply_log(const Entry& entry, Index entry_idx) = 0;

    /** Callback for persisting vote data
    * For safety reasons this callback MUST flush the change to disk. */
    virtual bmcl::Option<Error> persist_vote(NodeId node) = 0;

    /** Callback for persisting term data
    * For safety reasons this callback MUST flush the change to disk. */
    virtual bmcl::Option<Error> persist_term(TermId node) = 0;

    /** Callback for adding an entry to the log
    * For safety reasons this callback MUST flush the change to disk.
    * Return 0 on success.
    * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    virtual bmcl::Option<Error> push_back(const Entry& entry, Index entry_idx) = 0;

    /** Callback for removing the oldest entry from the log
    * For safety reasons this callback MUST flush the change to disk.
    * @note If memory was malloc'd in log_offer then this should be the right
    *  time to free the memory. */
    virtual void pop_front(const Entry& entry, Index entry_idx) = 0;

    /** Callback for removing the youngest entry from the log
    * For safety reasons this callback MUST flush the change to disk.
    * @note If memory was malloc'd in log_offer then this should be the right
    *  time to free the memory. */
    virtual void pop_back(const Entry& entry, Index entry_idx) = 0;

    /** Callback for catching debugging log messages
    * This callback is optional */
    virtual void log(const char *buf) = 0;
};


}
