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
        RAFT_NODE_VOTED_FOR_ME = 0,
        RAFT_NODE_VOTING       = 1,
        RAFT_NODE_HAS_SUFFICIENT_LOG = 2,
    };

public:
    inline explicit Node(node_id id) : _next_idx(1),  _match_idx(0), _id(id)
    {
        _flags.set(RAFT_NODE_VOTING, true);
    }

    inline node_id get_id() const { return _id; }

    inline std::size_t get_next_idx() const { return _next_idx; }
    inline void set_next_idx(std::size_t nextIdx) {/* log index begins at 1 */ _next_idx = nextIdx < 1 ? 1 : nextIdx; }

    inline std::size_t get_match_idx() const { return _match_idx; }
    inline void set_match_idx(std::size_t matchIdx) { _match_idx = matchIdx; }

    inline bool has_vote_for_me() const { return _flags.test(RAFT_NODE_VOTED_FOR_ME); }
    inline void vote_for_me(bool vote) { _flags.set(RAFT_NODE_VOTED_FOR_ME, vote); }

    inline void set_voting(bool voting) { _flags.set(RAFT_NODE_VOTING, voting); }
    inline bool is_voting() const { return _flags.test(RAFT_NODE_VOTING); }

    inline void set_has_sufficient_logs() { _flags.set(RAFT_NODE_HAS_SUFFICIENT_LOG, true); }
    inline bool has_sufficient_logs() const { return _flags.test(RAFT_NODE_HAS_SUFFICIENT_LOG); }

private:
    std::size_t _next_idx;
    std::size_t _match_idx;
    node_id _id;
    std::bitset<8> _flags;
};

class Nodes
{
public:
    using Items = std::vector<Node>;
    Nodes(node_id id, bool isVoting);
    inline std::size_t count() const { return _nodes.size(); }
    inline const Items& items() const { return _nodes; }
    inline node_id get_my_id() const { return _me; }
    inline bool is_me(node_id id) const { return _me == id; }
    void reset_all_votes();
    bmcl::Option<Node&> get_node(bmcl::Option<node_id> id);
    bmcl::Option<const Node&> get_node(node_id id) const;
    bmcl::Option<Node&> get_node(node_id id);
    const Node& get_my_node() const;
    Node& get_my_node();
    bmcl::Option<Node&> add_node(node_id id);
    bmcl::Option<Node&> add_non_voting_node(node_id id);
    void remove_node(node_id id);
    void remove_node(const bmcl::Option<Node&>& node);
    std::size_t get_nvotes_for_me(bmcl::Option<node_id> voted_for) const;
    std::size_t get_num_voting_nodes() const;
    bool raft_votes_is_majority(bmcl::Option<node_id> voted_for) const;
    static bool raft_votes_is_majority(std::size_t num_nodes, std::size_t nvotes);
    bool is_committed(std::size_t point) const;
private:
    node_id _me;
    Items _nodes;
};


}