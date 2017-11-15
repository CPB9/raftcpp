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
#include "Types.h"

namespace raft
{

class Node
{
    enum BitFlags
    {
        VotedForMe              = 0,
        NodeVoting              = 1,
        NodeHasSufficientLog    = 2,
    };

public:
    inline explicit Node(NodeId id) : _next_idx(1),  _match_idx(0), _id(id)
    {
        _flags.set(NodeVoting, true);
    }

    inline NodeId get_id() const { return _id; }

    inline std::size_t get_next_idx() const { return _next_idx; }
    inline void set_next_idx(std::size_t nextIdx) {/* log index begins at 1 */ _next_idx = nextIdx < 1 ? 1 : nextIdx; }

    inline std::size_t get_match_idx() const { return _match_idx; }
    inline void set_match_idx(std::size_t matchIdx) { _match_idx = matchIdx; }

    inline bool has_vote_for_me() const { return _flags.test(VotedForMe); }
    inline void vote_for_me(bool vote) { _flags.set(VotedForMe, vote); }

    inline void set_voting(bool voting) { _flags.set(NodeVoting, voting); }
    inline bool is_voting() const { return _flags.test(NodeVoting); }

    inline void set_has_sufficient_logs() { _flags.set(NodeHasSufficientLog, true); }
    inline bool has_sufficient_logs() const { return _flags.test(NodeHasSufficientLog); }

private:
    std::size_t _next_idx;
    std::size_t _match_idx;
    NodeId _id;
    std::bitset<8> _flags;
};

class Nodes
{
public:
    using Items = std::vector<Node>;
    Nodes(NodeId id, bool isVoting);
    inline std::size_t count() const { return _nodes.size(); }
    inline const Items& items() const { return _nodes; }
    inline NodeId get_my_id() const { return _me; }
    inline bool is_me(NodeId id) const { return _me == id; }
    void reset_all_votes();
    bmcl::Option<Node&> get_node(bmcl::Option<NodeId> id);
    bmcl::Option<const Node&> get_node(NodeId id) const;
    bmcl::Option<Node&> get_node(NodeId id);
    const Node& get_my_node() const;
    Node& get_my_node();
    bmcl::Option<Node&> add_node(NodeId id);
    bmcl::Option<Node&> add_non_voting_node(NodeId id);
    void remove_node(NodeId id);
    void remove_node(const bmcl::Option<Node&>& node);
    std::size_t get_nvotes_for_me(bmcl::Option<NodeId> voted_for) const;
    std::size_t get_num_voting_nodes() const;
    bool votes_has_majority(bmcl::Option<NodeId> voted_for) const;
    static bool votes_has_majority(std::size_t num_nodes, std::size_t nvotes);
    bool is_committed(std::size_t point) const;
private:
    NodeId _me;
    Items _nodes;
};


}