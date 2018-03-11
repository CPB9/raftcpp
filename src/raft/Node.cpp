#include <algorithm>
#include <assert.h>
#include "raft/Node.h"

namespace raft
{

Nodes::Nodes(NodeId id) : _me(id)
{
}

void Nodes::reset_all_votes()
{
    for (auto& i : _nodes)
        i.vote_for_me(false);
}

void Nodes::set_all_need_vote_req(bool need)
{
    for (Node& i: _nodes)
        i.set_need_vote_req(need);
}


void Nodes::set_all_need_pings(bool need)
{
    for (Node& i : _nodes)
        i.set_need_append_endtries_req(need);
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

bmcl::Option<const Node&> Nodes::get_my_node() const
{
    return get_node(_me);
}

Node& Nodes::add_node(NodeId id, bool is_voting)
{   /* set to voting if node already exists */
    bmcl::Option<Node&> node = get_node(id);
    if (node.isSome())
    {
        if (is_voting)
            node->set_voting(true);
        return node.unwrap();
    }

    _nodes.emplace_back(Node(id, is_me(id)));
    _nodes.back().set_voting(is_voting);
    std::sort(_nodes.begin(), _nodes.end(), [](const Node& l, const Node& r) { return l.get_id() < r.get_id(); });
    return *std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& n) { return n.get_id() == id; });
}

Node& Nodes::add_my_node(bool is_voting)
{
    return add_node(_me, is_voting);
}

void Nodes::remove_node(NodeId id)
{
    const auto i = std::find_if(_nodes.begin(), _nodes.end(), [id](const Node& i) {return i.get_id() == id; });
    if (i != _nodes.end())
        _nodes.erase(i);
}

NodeCount Nodes::get_nvotes_for_me(bmcl::Option<NodeId> voted_for) const
{
    NodeCount votes = std::count_if(_nodes.begin(), _nodes.end(), [](const Node& i) { return !i.is_me() && i.is_voting() && i.has_vote_for_me(); });

    if (voted_for == _me)
        votes += 1;

    return votes;
}

NodeCount Nodes::get_num_voting_nodes() const
{
    return std::count_if(_nodes.begin(), _nodes.end(), [](const Node& i) { return i.is_voting(); });
}

bool Nodes::votes_has_majority(bmcl::Option<NodeId> voted_for) const
{
    return votes_has_majority(get_num_voting_nodes(), get_nvotes_for_me(voted_for));
}

bool Nodes::votes_has_majority(NodeCount num_nodes, NodeCount nvotes)
{
    if (num_nodes < nvotes)
        return false;
    return num_nodes / 2 < nvotes;
}

bool Nodes::is_committed(Index idx) const
{
    NodeCount votes = std::count_if(_nodes.begin(), _nodes.end(), [idx](const Node& i) { return i.is_voting() && idx <= i.get_match_idx(); });
    return (get_num_voting_nodes() / 2 < votes);
}

bool Nodes::is_me_the_only_voting() const
{
    bmcl::Option<const Node&> node = get_my_node();
    if (node.isNone() || !node->is_voting())
        return false;
    return (get_num_voting_nodes() == 1);
}

bool Nodes::is_me_candidate_ready() const
{
    bmcl::Option<const Node&> node = get_my_node();
    if (node.isNone() || !node->is_voting())
        return false;
    return (get_num_voting_nodes() > 1);
}

}
