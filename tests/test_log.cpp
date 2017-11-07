#include <deque>
#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"

using namespace raft;

TEST(TestLog, new_is_empty)
{
    raft::Logger l;
    EXPECT_EQ(0, l.count());
}

TEST(TestLog, append_is_not_empty)
{
    raft::Logger l;

    l.append(raft_entry_t(0, 1));
    EXPECT_EQ(1, l.count());
}

TEST(TestLog, get_at_idx)
{
    raft::Logger l;
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.append(e1);
    l.append(e2);
    l.append(e3);
    EXPECT_EQ(3, l.count());

    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.get_at_idx(2).isSome());
    EXPECT_EQ(e2.id, l.get_at_idx(2).unwrap().id);
}

TEST(TestLog, get_at_idx_returns_null_where_out_of_bounds)
{
    raft::Logger l;

    l.append(raft_entry_t(0, 1));
    EXPECT_FALSE(l.get_at_idx(2).isSome());
}

TEST(TestLog, delete)
{
    raft::Server r(raft::node_id(1), true);
    raft::Logger l;
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    std::deque<raft_entry_t> queue;

    raft_cbs_t funcs = {0};
    funcs.log_pop = [&queue](const raft::Server* raft, const raft_entry_t& entry, std::size_t entry_idx) -> int
    {
        queue.push_back(entry);
        return 0;
    };
    r.set_callbacks(funcs);

    l.append(e1);
    l.append(e2);
    l.append(e3);
    EXPECT_EQ(3, l.count());

    l.log_delete_from(&r, 3);

    EXPECT_EQ(queue.front().id, e3.id);
    queue.pop_front();

    EXPECT_EQ(2, l.count());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
    l.log_delete_from(&r, 2);
    EXPECT_EQ(1, l.count());
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    l.log_delete_from(&r, 1);
    EXPECT_EQ(0, l.count());
    EXPECT_FALSE(l.get_at_idx(1).isSome());
}

TEST(TestLog, delete_onwards)
{
    raft::Logger l;
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.append(e1);
    l.append(e2);
    l.append(e3);
    EXPECT_EQ(3, l.count());

    /* even 3 gets deleted */
    l.log_delete_from(nullptr, 2);
    EXPECT_EQ(1, l.count());
    EXPECT_TRUE(l.get_at_idx(1).isSome());
    EXPECT_EQ(e1.id, l.get_at_idx(1).unwrap().id);
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
}

TEST(TestLog, peektail)
{
    raft::Logger l;
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.append(e1);
    l.append(e2);
    l.append(e3);
    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.peektail().isSome());
    EXPECT_EQ(e3.id, l.peektail().unwrap().id);
}

TEST(TestLog, cant_append_duplicates)
{
    raft::Logger l;
    l.append(raft::raft_entry_t(1, 1));
    EXPECT_EQ(1, l.count());
    l.append(raft::raft_entry_t(1, 1));
    EXPECT_EQ(1, l.count());
}

TEST(TestLogCommitter, wont_apply_entry_if_we_dont_have_entry_to_apply)
{
    raft::LogCommitter lc;
    lc.set_commit_idx(0);
    lc.set_last_applied_idx(0);

    lc.entry_apply_one(nullptr);
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}

TEST(TestLogCommitter, wont_apply_entry_if_there_isnt_a_majority)
{
    raft::LogCommitter lc;
    lc.set_commit_idx(0);
    lc.set_last_applied_idx(0);

    auto e = lc.entry_apply_one(nullptr);
    EXPECT_TRUE(e.isNone());
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());

    lc.entry_append(nullptr, raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    lc.entry_apply_one(nullptr);
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}