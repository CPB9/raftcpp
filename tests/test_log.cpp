#include <deque>
#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"

using namespace raft;

TEST(TestLog, new_is_empty)
{
    raft::LogCommitter l(nullptr, nullptr);
    EXPECT_EQ(0, l.count());
}

TEST(TestLog, append_is_not_empty)
{
    raft::LogCommitter l(nullptr, nullptr);
    l.entry_append(raft_entry_t(0, 1));
    EXPECT_EQ(1, l.count());
}

TEST(TestLog, get_at_idx)
{
    raft::LogCommitter l(nullptr, nullptr);
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.get_at_idx(2).isSome());
    EXPECT_EQ(e2.id, l.get_at_idx(2).unwrap().id);
}

TEST(TestLog, get_at_idx_returns_null_where_out_of_bounds)
{
    raft::LogCommitter l(nullptr, nullptr);

    l.entry_append(raft_entry_t(0, 1));
    EXPECT_FALSE(l.get_at_idx(2).isSome());
}

class TestSaver : public Saver
{
public:
    std::deque<raft_entry_t> queue;
    void log_pop(const raft_entry_t& entry, std::size_t entry_idx) override
    {
        queue.push_back(entry);
    }
};

TEST(TestLog, delete)
{
    TestSaver saver;
    raft::Server r(raft::node_id(1), true);
    raft::LogCommitter l(&r, &saver);
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    l.entry_delete_from_idx(3);

    EXPECT_EQ(saver.queue.front().id, e3.id);

    EXPECT_EQ(2, l.count());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
    l.entry_delete_from_idx(2);
    EXPECT_EQ(1, l.count());
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    l.entry_delete_from_idx(1);
    EXPECT_EQ(0, l.count());
    EXPECT_FALSE(l.get_at_idx(1).isSome());
}

TEST(TestLog, delete_onwards)
{
    TestSaver saver;
    raft::Server r(raft::node_id(1), true);
    raft::LogCommitter l(&r, &saver);
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    /* even 3 gets deleted */
    l.entry_delete_from_idx(2);
    EXPECT_EQ(1, l.count());
    EXPECT_TRUE(l.get_at_idx(1).isSome());
    EXPECT_EQ(e1.id, l.get_at_idx(1).unwrap().id);
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
}

TEST(TestLog, peektail)
{
    raft::LogCommitter l(nullptr, nullptr);
    raft_entry_t e1(0, 1), e2(0, 2), e3(0, 3);

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.back().isSome());
    EXPECT_EQ(e3.id, l.back().unwrap().id);
}

TEST(TestLog, cant_append_duplicates)
{
    raft::LogCommitter l(nullptr, nullptr);
    l.entry_append(raft::raft_entry_t(1, 1));
    EXPECT_EQ(1, l.count());
    l.entry_append(raft::raft_entry_t(1, 1));
    EXPECT_EQ(1, l.count());
}

TEST(TestLogCommitter, wont_apply_entry_if_we_dont_have_entry_to_apply)
{
    raft::LogCommitter lc(nullptr, nullptr);
    lc.entry_apply_one();
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}

TEST(TestLogCommitter, wont_apply_entry_if_there_isnt_a_majority)
{
    raft::LogCommitter lc(nullptr, nullptr);

    auto e = lc.entry_apply_one();
    EXPECT_TRUE(e.isNone());
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());

    lc.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    lc.entry_apply_one();
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}