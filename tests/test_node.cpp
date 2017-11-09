#include <gtest/gtest.h>
#include "raft/Node.h"

using namespace raft;

TEST(TestNode, is_voting_by_default)
{
    raft::Node p((raft::node_id)1);
    EXPECT_TRUE(p.is_voting());
}

TEST(TestNode, node_set_nextIdx)
{
    raft::Node p((raft::node_id)1);
    p.set_next_idx(3);
    EXPECT_EQ(3, p.get_next_idx());
}

TEST(TestNode, cfg_sets_num_nodes)
{
    raft::Nodes nodes(raft::node_id(1), true);
    nodes.add_node(raft::node_id(2));

    EXPECT_EQ(2, nodes.count());
}

TEST(TestNode, cant_get_node_we_dont_have)
{
    raft::Nodes nodes(raft::node_id(1), true);
    nodes.add_node(raft::node_id(2));

    EXPECT_FALSE(nodes.get_node(raft::node_id(0)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::node_id(1)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::node_id(2)).isSome());
    EXPECT_FALSE(nodes.get_node(raft::node_id(3)).isSome());
}

TEST(TestNode, add_node_makes_non_voting_node_voting)
{
    raft::Nodes nodes(raft::node_id(9), false);
    bmcl::Option<raft::Node&> n1 = nodes.get_node(raft::node_id(9));

    EXPECT_TRUE(n1.isSome());
    EXPECT_FALSE(n1->is_voting());
    nodes.add_node(raft::node_id(9));
    EXPECT_TRUE(n1->is_voting());
    EXPECT_EQ(1, nodes.count());
}

TEST(TestNode, add_node_with_already_existing_id_doesnt_add_new_one)
{
    raft::Nodes nodes(raft::node_id(1), true);
    nodes.add_node(raft::node_id(9));
    nodes.add_node(raft::node_id(11));

    const auto& node = nodes.add_node(raft::node_id(9));
    EXPECT_TRUE(node.isSome());
    EXPECT_EQ(node->get_id(), raft::node_id(9));
    EXPECT_TRUE(node->is_voting());
}

TEST(TestNode, add_non_voting_node_with_already_existing_id_is_not_allowed)
{
    raft::Nodes nodes(raft::node_id(1), true);
    nodes.add_non_voting_node(raft::node_id(9));
    nodes.add_non_voting_node(raft::node_id(11));

    EXPECT_FALSE(nodes.add_non_voting_node(raft::node_id(9)).isSome());
    EXPECT_FALSE(nodes.add_non_voting_node(raft::node_id(11)).isSome());
}

TEST(TestNode, add_non_voting_node_with_already_existing_voting_id_is_not_allowed)
{
    raft::Nodes nodes(raft::node_id(1), true);
    nodes.add_node(raft::node_id(9));
    nodes.add_node(raft::node_id(11));

    EXPECT_FALSE(nodes.add_non_voting_node(raft::node_id(9)).isSome());
    EXPECT_FALSE(nodes.add_non_voting_node(raft::node_id(11)).isSome());
}

TEST(TestNode, remove_node)
{
    raft::Nodes nodes(raft::node_id(1), true);
    bmcl::Option<raft::Node&> n1 = nodes.add_node(raft::node_id(2));
    bmcl::Option<raft::Node&> n2 = nodes.add_node(raft::node_id(9));

    nodes.remove_node(raft::node_id(2));
    EXPECT_FALSE(nodes.get_node(raft::node_id(2)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::node_id(9)).isSome());
    nodes.remove_node(raft::node_id(9));
    EXPECT_FALSE(nodes.get_node(raft::node_id(9)).isSome());
}
