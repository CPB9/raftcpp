#pragma once
#include <algorithm>
#include <assert.h>
#include "Node.h"

namespace raft
{
    
Nodes::Nodes(NodeId id, bool isVoting) : _me(id)
{
    auto r = add_node(id);
    assert(r.isSome());
    r.unwrap().set_voting(isVoting);
}

void Nodes::reset_all_votes()
{
    for (auto& i : _nodes)
        i.vote_for_me(false);
}

bmcl::Option<Node&> Nodes::get_node(bmcl::Option<NodeId> id)
{
    if (id.isNone()) return bmcl::None;
    return get_node(id.unwrap());
}

bmcl::Option<const Node&> Nodes::get_node(NodeId id) const
{
    const auto& i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
    if (i == _nodes.end())
        return bmcl::None;
    return *i;
}

bmcl::Option<Node&> Nodes::get_node(NodeId id)
{
    const auto& i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
    if (i == _nodes.end())
        return bmcl::None;
    return *i;
}

const Node& Nodes::get_my_node() const
{
    const auto& n = get_node(_me);
    assert(n.isSome());
    return n.unwrap();
}

Node& Nodes::get_my_node()
{
    bmcl::Option<Node&> n = get_node(_me);
    assert(n.isSome());
    return n.unwrap();
}

bmcl::Option<Node&> Nodes::add_node(NodeId id)
{   /* set to voting if node already exists */
    bmcl::Option<Node&> node = get_node(id);
    if (node.isSome())
    {
        node->set_voting(true);
        return node;
    }

    _nodes.emplace_back(Node(id));
    return _nodes.back();
}

bmcl::Option<Node&> Nodes::add_non_voting_node(NodeId id)
{
    if (get_node(id).isSome())
        return bmcl::None;

    bmcl::Option<Node&> node = add_node(id);
    if (node.isNone())
        return bmcl::None;

    node->set_voting(false);
    return node;
}

void Nodes::remove_node(NodeId id)
{
    assert(id != _me);
    const auto i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
    assert(i != _nodes.end());
    _nodes.erase(i);
}

void Nodes::remove_node(const bmcl::Option<Node&>& node)
{
    if (node.isNone())
        return;
    remove_node(node->get_id());
}

std::size_t Nodes::get_nvotes_for_me(bmcl::Option<NodeId> voted_for) const
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

std::size_t Nodes::get_num_voting_nodes() const
{
    std::size_t num = 0;
    for (const Node& i : _nodes)
        if (i.is_voting())
            num++;
    return num;
}

bool Nodes::votes_has_majority(bmcl::Option<NodeId> voted_for) const
{
    return votes_has_majority(get_num_voting_nodes(), get_nvotes_for_me(voted_for));
}

bool Nodes::votes_has_majority(std::size_t num_nodes, std::size_t nvotes)
{
    if (num_nodes < nvotes)
        return false;
    std::size_t half = num_nodes / 2;
    return half + 1 <= nvotes;
}

bool Nodes::is_committed(Index idx) const
{
    std::size_t votes = 1;
    for (const Node& i : _nodes)
    {
        if (!is_me(i.get_id()) && i.is_voting() && idx <= i.get_match_idx())
        {
            votes++;
        }
    }

    return (get_num_voting_nodes() / 2 < votes);
}

}
