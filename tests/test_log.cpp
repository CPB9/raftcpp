#include <deque>
#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"


TEST(TestLog, new_is_empty)
{
    RaftLog l;
    EXPECT_EQ(0, l.log_count());
}

TEST(TestLog, append_is_not_empty)
{
    RaftLog l;
    raft_entry_t e;

    e.id = 1;

    EXPECT_FALSE(l.log_append_entry(nullptr, e).isSome());
    EXPECT_EQ(1, l.log_count());
}

TEST(TestLog, get_at_idx)
{
    RaftLog l;
    raft_entry_t e1, e2, e3;

    e1.id = 1;
    EXPECT_FALSE(l.log_append_entry(nullptr, e1).isSome());
    e2.id = 2;
    EXPECT_FALSE(l.log_append_entry(nullptr, e2).isSome());
    e3.id = 3;
    EXPECT_FALSE(l.log_append_entry(nullptr, e3).isSome());
    EXPECT_EQ(3, l.log_count());

    EXPECT_EQ(3, l.log_count());
    EXPECT_TRUE(l.log_get_at_idx(2).isSome());
    EXPECT_EQ(e2.id, l.log_get_at_idx(2).unwrap().id);
}

TEST(TestLog, get_at_idx_returns_null_where_out_of_bounds)
{
    RaftLog l;
    raft_entry_t e1;

    e1.id = 1;
    EXPECT_FALSE(l.log_append_entry(nullptr, e1).isSome());
    EXPECT_FALSE(l.log_get_at_idx(2).isSome());
}

TEST(TestLog, delete)
{
    Raft r(raft_node_id(1), true);
    RaftLog l;
    raft_entry_t e1, e2, e3;

    std::deque<raft_entry_t> queue;

    raft_cbs_t funcs = {0};
    funcs.log_pop = [&queue](const Raft* raft, const raft_entry_t& entry, int entry_idx) -> int
    {
        queue.push_back(entry);
        return 0;
    };
    r.set_callbacks(funcs);

    e1.id = 1;
    EXPECT_FALSE(l.log_append_entry(nullptr, e1).isSome());
    e2.id = 2;
    EXPECT_FALSE(l.log_append_entry(nullptr, e2).isSome());
    e3.id = 3;
    EXPECT_FALSE(l.log_append_entry(nullptr, e3).isSome());
    EXPECT_EQ(3, l.log_count());

    l.log_delete(&r, 3);

    raft_entry_t e = queue.front();
    queue.pop_front();
    unsigned int id = e.id;
    EXPECT_EQ(id, e3.id);

    EXPECT_EQ(2, l.log_count());
    EXPECT_FALSE(l.log_get_at_idx(3).isSome());
    l.log_delete(&r, 2);
    EXPECT_EQ(1, l.log_count());
    EXPECT_FALSE(l.log_get_at_idx(2).isSome());
    l.log_delete(&r, 1);
    EXPECT_EQ(0, l.log_count());
    EXPECT_FALSE(l.log_get_at_idx(1).isSome());
}

TEST(TestLog, delete_onwards)
{
    RaftLog l;
    raft_entry_t e1, e2, e3;

    e1.id = 1;
    EXPECT_FALSE(l.log_append_entry(nullptr, e1).isSome());
    e2.id = 2;
    EXPECT_FALSE(l.log_append_entry(nullptr, e2).isSome());
    e3.id = 3;
    EXPECT_FALSE(l.log_append_entry(nullptr, e3).isSome());
    EXPECT_EQ(3, l.log_count());

    /* even 3 gets deleted */
    l.log_delete(nullptr, 2);
    EXPECT_EQ(1, l.log_count());
    EXPECT_TRUE(l.log_get_at_idx(1).isSome());
    EXPECT_EQ(e1.id, l.log_get_at_idx(1).unwrap().id);
    EXPECT_FALSE(l.log_get_at_idx(2).isSome());
    EXPECT_FALSE(l.log_get_at_idx(3).isSome());
}

TEST(TestLog, peektail)
{
    RaftLog l;
    raft_entry_t e1, e2, e3;

    e1.id = 1;
    EXPECT_FALSE(l.log_append_entry(nullptr, e1).isSome());
    e2.id = 2;
    EXPECT_FALSE(l.log_append_entry(nullptr, e2).isSome());
    e3.id = 3;
    EXPECT_FALSE(l.log_append_entry(nullptr, e3).isSome());
    EXPECT_EQ(3, l.log_count());
    EXPECT_TRUE(l.log_peektail().isSome());
    EXPECT_EQ(e3.id, l.log_peektail().unwrap().id);
}

#if 0
// TODO: duplicate testing not implemented yet
void T_estlog_cant_append_duplicates(CuTest * tc)
{
    log_t *l;
    raft_entry_t e;

    e.id = 1;

    l = log_new();
    EXPECT_EQ(1, log_append_entry(l, &e));
    EXPECT_EQ(1, log_count(l));
}
#endif

