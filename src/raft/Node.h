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
#include <algorithm>
#include <assert.h>

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
    explicit Node(node_id id)
    {
        _me.next_idx = 1;
        _me.match_idx = 0;
        _me.id = id;
        _me.flags.set(RAFT_NODE_VOTING, true);
    }

    node_id get_id() const { return _me.id; }

    std::size_t get_next_idx() const { return _me.next_idx; }
    void set_next_idx(std::size_t nextIdx) {/* log index begins at 1 */ _me.next_idx = nextIdx < 1 ? 1 : nextIdx; }

    std::size_t get_match_idx() const { return _me.match_idx; }
    void set_match_idx(std::size_t matchIdx) { _me.match_idx = matchIdx; }

    bool has_vote_for_me() const { return _me.flags.test(RAFT_NODE_VOTED_FOR_ME); }
    void vote_for_me(bool vote) { _me.flags.set(RAFT_NODE_VOTED_FOR_ME, vote); }

    void set_voting(bool voting) { _me.flags.set(RAFT_NODE_VOTING, voting); }
    bool is_voting() const { return _me.flags.test(RAFT_NODE_VOTING); }

    void set_has_sufficient_logs() { _me.flags.set(RAFT_NODE_HAS_SUFFICIENT_LOG, true); }
    bool has_sufficient_logs() const { return _me.flags.test(RAFT_NODE_HAS_SUFFICIENT_LOG); }

private:
    struct node_private_t
    {
        std::size_t next_idx;
        std::size_t match_idx;
        node_id id;
        std::bitset<8> flags;
    };
    node_private_t _me;
};

class Nodes
{
public:
    using Items = std::vector<Node>;
    Nodes(node_id id, bool isVoting) : _me(id)
    {
        auto r = add_node(id);
        assert(r.isSome());
        r.unwrap().set_voting(isVoting);
    }
    std::size_t count() const { return _nodes.size(); }
    const Items& items() const { return _nodes; }
    node_id get_my_id() const { return _me; }
    bool is_me(node_id id) const { return _me == id; }
    void reset_all_votes()
    {
        for (auto& i : _nodes)
            i.vote_for_me(false);
    }

//     const Node& get_my_node();
//     bmcl::Option<Node&> add_node(node_id id);
//     bmcl::Option<Node&> add_non_voting_node(node_id id);
//     void remove_node(node_id id);
//     void remove_node(const bmcl::Option<Node&>& node);
//     bmcl::Option<Node&> get_node(node_id id);
//     bmcl::Option<Node&> get_node(bmcl::Option<node_id> id);
//     std::size_t get_num_nodes() const { return _nodes.size(); }

    bmcl::Option<Node&> get_node(bmcl::Option<node_id> id)
    {
        if (id.isNone()) return bmcl::None;
        return get_node(id.unwrap());
    }

    bmcl::Option<const Node&> get_node(node_id id) const
    {
        auto& i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
        if (i == _nodes.end())
            return bmcl::None;
        return *i;
    }

    bmcl::Option<Node&> get_node(node_id id)
    {
        auto& i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
        if (i == _nodes.end())
            return bmcl::None;
        return *i;
    }

    const Node& get_my_node() const
    {
        const auto& n = get_node(_me);
        assert(n.isSome());
        return n.unwrap();
    }

    Node& get_my_node()
    {
        auto& n = get_node(_me);
        assert(n.isSome());
        return n.unwrap();
    }

    bmcl::Option<Node&> add_node(node_id id)
    {   /* set to voting if node already exists */
        bmcl::Option<Node&> node = get_node(id);
        if (node.isSome())
        {
            if (node->is_voting())
                return bmcl::None;
            node->set_voting(true);
            return node;
        }

        _nodes.emplace_back(Node(id));
        return _nodes.back();
    }

    bmcl::Option<Node&> add_non_voting_node(node_id id)
    {
        if (get_node(id).isSome())
            return bmcl::None;

        bmcl::Option<Node&> node = add_node(id);
        if (node.isNone())
            return bmcl::None;

        node->set_voting(false);
        return node;
    }

    void remove_node(node_id id)
    {
        assert(id != _me);
        auto i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
        assert(i != _nodes.end());
        _nodes.erase(i);
    }

    void remove_node(const bmcl::Option<Node&>& node)
    {
        if (node.isNone())
            return;
        remove_node(node->get_id());
    }

    std::size_t get_nvotes_for_me(bmcl::Option<node_id> voted_for) const
    {
        //std::count_if(_nodes.begin(), _nodes.end(), [_me](const Node& i) { return _});
        std::size_t votes = 0;

        for (const Node& i : _nodes)
        {
            if (_me != i.get_id() && i.is_voting() && i.has_vote_for_me())
                votes += 1;
        }

        if (voted_for == _me)
            votes += 1;

        return votes;
    }

    std::size_t get_num_voting_nodes() const
    {
        std::size_t num = 0;
        for (const Node& i : _nodes)
            if (i.is_voting())
                num++;
        return num;
    }

    bool raft_votes_is_majority(bmcl::Option<node_id> voted_for) const
    {
        return raft_votes_is_majority(get_num_voting_nodes(), get_nvotes_for_me(voted_for));
    }

    static bool raft_votes_is_majority(std::size_t num_nodes, std::size_t nvotes)
    {
        if (num_nodes < nvotes)
            return false;
        std::size_t half = num_nodes / 2;
        return half + 1 <= nvotes;
    }

private:
    node_id _me;
    Items _nodes;
};


}