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
