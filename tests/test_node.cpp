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
    Nodes ns(NodeId(1));
    EXPECT_EQ(NodeId(1), ns.get_my_id());
    EXPECT_TRUE(ns.is_me(NodeId(1)));
    EXPECT_FALSE(ns.is_me(NodeId(2)));
}

TEST(TestNode, my_id_maybe_not_always_in_list)
{
    Nodes nodes(NodeId(1));
    EXPECT_EQ(0, nodes.count());
    EXPECT_FALSE(nodes.get_node(NodeId(1)).isSome());

    nodes.add_my_node(true);
    EXPECT_EQ(1, nodes.count());
    EXPECT_TRUE(nodes.get_node(NodeId(1)).isSome());
}

TEST(TestNode, cant_get_node_we_dont_have)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(true);
    nodes.add_node(NodeId(2), true);

    EXPECT_FALSE(nodes.get_node(NodeId(0)).isSome());
    EXPECT_TRUE(nodes.get_node(NodeId(1)).isSome());
    EXPECT_TRUE(nodes.get_node(NodeId(2)).isSome());
    EXPECT_FALSE(nodes.get_node(NodeId(3)).isSome());
}

TEST(TestNode, add_node_makes_non_voting_node_voting)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(false);
    bmcl::Option<const raft::Node&> n1 = nodes.get_node(NodeId(1));

    EXPECT_TRUE(n1.isSome());
    EXPECT_FALSE(n1->is_voting());
    nodes.add_node(NodeId(1), true);
    EXPECT_TRUE(n1->is_voting());
    EXPECT_EQ(1, nodes.count());
}

TEST(TestNode, add_node_with_already_existing_id_doesnt_add_new_one)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(true);
    nodes.add_node(NodeId(9), true);
    nodes.add_node(NodeId(11), true);

    const auto& node = nodes.add_node(NodeId(9), true);
    EXPECT_EQ(node.get_id(), NodeId(9));
    EXPECT_TRUE(node.is_voting());
}

TEST(TestNode, add_non_voting_node_with_already_existing_id_doesnt_change_voting)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(true);
    nodes.add_node(NodeId(2), false);
    nodes.add_node(NodeId(3), false);

    EXPECT_FALSE(nodes.add_node(NodeId(2), false).is_voting());
    EXPECT_FALSE(nodes.add_node(NodeId(3), false).is_voting());
}

TEST(TestNode, add_non_voting_node_with_already_existing_voting_doesnt_change_voting)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(true);
    nodes.add_node(NodeId(2), true);
    nodes.add_node(NodeId(3), true);

    EXPECT_TRUE(nodes.add_node(NodeId(2), true).is_voting());
    EXPECT_TRUE(nodes.add_node(NodeId(3), true).is_voting());
}

TEST(TestNode, remove_node)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(true);
    nodes.add_node(NodeId(2), true);
    nodes.add_node(NodeId(3), true);

    nodes.remove_node(NodeId(2));
    EXPECT_FALSE(nodes.get_node(NodeId(2)).isSome());
    EXPECT_TRUE(nodes.get_node(NodeId(3)).isSome());
    nodes.remove_node(NodeId(3));
    EXPECT_FALSE(nodes.get_node(NodeId(3)).isSome());
}

TEST(TestNode, Rediness)
{
    Nodes nodes(NodeId(1));
    nodes.add_my_node(false);
    nodes.add_node(NodeId(2), true);
    EXPECT_FALSE(nodes.is_me_candidate_ready());
    EXPECT_FALSE(nodes.is_me_the_only_voting());

    nodes.get_node(NodeId(1))->set_voting(true);
    nodes.get_node(NodeId(2))->set_voting(false);
    EXPECT_FALSE(nodes.is_me_candidate_ready());
    EXPECT_TRUE(nodes.is_me_the_only_voting());

    nodes.add_node(NodeId(3), true);
    EXPECT_TRUE(nodes.is_me_candidate_ready());
}