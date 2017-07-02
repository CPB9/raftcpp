#include <gtest/gtest.h>
#include "raft/Node.h"

TEST(TestNode, is_voting_by_default)
{
    RaftNode p((void*)1, (raft_node_id)1);
    EXPECT_TRUE(p.raft_node_is_voting());
}

TEST(TestNode, node_set_nextIdx)
{
    RaftNode p((void*)1, (raft_node_id)1);
    p.raft_node_set_next_idx(3);
    EXPECT_EQ(3, p.raft_node_get_next_idx());
}
