#include <gtest/gtest.h>
#include "raft/Node.h"

using namespace raft;

TEST(TestNode, is_voting_by_default)
{
    raft::Node p(NodeId(1), true);
    EXPECT_TRUE(p.is_voting());
}

TEST(TestNode, node_set_nextIdx)
{
    raft::Node p(NodeId(1), true);
    p.set_next_idx(3);
    EXPECT_EQ(3, p.get_next_idx());
}

TEST(TestNode, get_my_node)
{
    raft::Nodes ns(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2) });
    EXPECT_EQ(raft::NodeId(1), ns.get_my_id());
    EXPECT_TRUE(ns.is_me(raft::NodeId(1)));
    EXPECT_FALSE(ns.is_me(raft::NodeId(2)));
}

TEST(TestNode, my_id_is_always_in_list)
{
    raft::Nodes nodes(raft::NodeId(1), { NodeId(2), NodeId(3) });
    EXPECT_EQ(3, nodes.count());
    EXPECT_TRUE(nodes.get_node(raft::NodeId(1)).isSome());
}

TEST(TestNode, cfg_sets_num_nodes)
{
    raft::Nodes nodes(raft::NodeId(1), { NodeId(1), NodeId(2)});
    EXPECT_EQ(2, nodes.count());
}

TEST(TestNode, cant_get_node_we_dont_have)
{
    raft::Nodes nodes(raft::NodeId(1), { NodeId(1), NodeId(2) });
    EXPECT_FALSE(nodes.get_node(raft::NodeId(0)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::NodeId(1)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::NodeId(2)).isSome());
    EXPECT_FALSE(nodes.get_node(raft::NodeId(3)).isSome());
}

TEST(TestNode, add_node_makes_non_voting_node_voting)
{
    raft::Nodes nodes(raft::NodeId(9), false);
    bmcl::Option<raft::Node&> n1 = nodes.get_node(raft::NodeId(9));

    EXPECT_TRUE(n1.isSome());
    EXPECT_FALSE(n1->is_voting());
    nodes.add_node(raft::NodeId(9), true);
    EXPECT_TRUE(n1->is_voting());
    EXPECT_EQ(1, nodes.count());
}

TEST(TestNode, add_node_with_already_existing_id_doesnt_add_new_one)
{
    raft::Nodes nodes(raft::NodeId(1), true);
    nodes.add_node(raft::NodeId(9), true);
    nodes.add_node(raft::NodeId(11), true);

    const auto& node = nodes.add_node(raft::NodeId(9), true);
    EXPECT_EQ(node.get_id(), raft::NodeId(9));
    EXPECT_TRUE(node.is_voting());
}

TEST(TestNode, add_non_voting_node_with_already_existing_id_doesnt_change_voting)
{
    raft::Nodes nodes(raft::NodeId(1), true);
    nodes.add_node(raft::NodeId(2), false);
    nodes.add_node(raft::NodeId(3), false);

    EXPECT_FALSE(nodes.add_node(raft::NodeId(2), false).is_voting());
    EXPECT_FALSE(nodes.add_node(raft::NodeId(3), false).is_voting());
}

TEST(TestNode, add_non_voting_node_with_already_existing_voting_doesnt_change_voting)
{
    raft::Nodes nodes(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2), raft::NodeId(3) });

    EXPECT_TRUE(nodes.add_node(raft::NodeId(2), true).is_voting());
    EXPECT_TRUE(nodes.add_node(raft::NodeId(3), true).is_voting());
}

TEST(TestNode, remove_node)
{
    raft::Nodes nodes(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2), raft::NodeId(3) });

    nodes.remove_node(raft::NodeId(2));
    EXPECT_FALSE(nodes.get_node(raft::NodeId(2)).isSome());
    EXPECT_TRUE(nodes.get_node(raft::NodeId(3)).isSome());
    nodes.remove_node(raft::NodeId(3));
    EXPECT_FALSE(nodes.get_node(raft::NodeId(3)).isSome());
}
