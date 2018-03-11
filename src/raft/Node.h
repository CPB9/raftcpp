/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Representation of a peer
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */
#pragma once
#include <bitset>
#include <bmcl/ArrayView.h>
#include "raft/Types.h"

namespace raft
{

class Node
{
    enum BitFlags
    {
        VotedForMe              = 0,
        NodeVoting              = 1,
        NeedVoteReq             = 2,
        NeedAppendEntriesReq    = 3,
        IsMe                    = 4,
    };

public:
    inline explicit Node(NodeId id, bool is_me) : _id(id), _next_idx(1),  _match_idx(0), _last_cfg_seen_idx(0), _flags(0)
    {
        _flags.set(NodeVoting, true);
        _flags.set(IsMe, is_me);
    }

    inline NodeId get_id() const { return _id; }
    inline bool is_me() const { return _flags.test(IsMe); }

    inline Index get_next_idx() const { return _next_idx; }
    inline void set_next_idx(Index idx) {/* log index begins at 1 */ _next_idx = idx < 1 ? 1 : idx; }

    inline Index get_match_idx() const { return _match_idx; }
    inline void set_match_idx(Index idx) { _match_idx = idx; }

    inline Index get_last_cfg_seen_idx() const { return _last_cfg_seen_idx; }
    inline void set_last_cfg_seen_idx(Index idx) { _last_cfg_seen_idx = idx; }

    inline bool has_vote_for_me() const { return _flags.test(VotedForMe); }
    inline void vote_for_me(bool vote) { _flags.set(VotedForMe, vote); }

    inline void set_voting(bool voting) { _flags.set(NodeVoting, voting); }
    inline bool is_voting() const { return _flags.test(NodeVoting); }

    inline void set_need_vote_req(bool need) { _flags.set(NeedVoteReq, need); }
    inline bool need_vote_req() const { return _flags.test(NeedVoteReq); }

    inline void set_need_append_endtries_req(bool need) { _flags.set(NeedAppendEntriesReq, need); }
    inline bool need_append_endtries_req() const { return _flags.test(NeedAppendEntriesReq); }

private:
    NodeId          _id;
    Index           _next_idx;
    Index           _match_idx;
    Index           _last_cfg_seen_idx;
    std::bitset<8>  _flags;
};

class Nodes
{
public:
    using Items = std::vector<Node>;
    Nodes(NodeId id);
    inline NodeCount count() const { return _nodes.size(); }
    inline const Items& items() const { return _nodes; }
    inline NodeId get_my_id() const { return _me; }
    inline bool is_me(NodeId id) const { return _me == id; }
    void reset_all_votes();
    void set_all_need_vote_req(bool need);
    void set_all_need_pings(bool need);
    bmcl::Option<const Node&> get_node(NodeId id) const;
    bmcl::Option<Node&> get_node(NodeId id);
    bmcl::Option<const Node&> get_my_node() const;
    Node& add_node(NodeId id, bool is_voting);
    Node& add_my_node(bool is_voting);
    void remove_node(NodeId id);
    bool is_me_the_only_voting() const;
    bool is_me_candidate_ready() const;
    NodeCount get_nvotes_for_me(bmcl::Option<NodeId> voted_for) const;
    NodeCount get_num_voting_nodes() const;
    bool votes_has_majority(bmcl::Option<NodeId> voted_for) const;
    static bool votes_has_majority(NodeCount num_nodes, NodeCount nvotes);
    bool is_committed(Index idx) const;
private:
    NodeId _me;
    Items  _nodes;
};


}