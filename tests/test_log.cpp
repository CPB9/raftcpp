#include <deque>
#include <gtest/gtest.h>
#include "raft/Log.h"
#include "mock_send_functions.h"

using namespace raft;

TEST(TestLog, new_is_empty)
{
    raft::LogCommitter l(nullptr);
    EXPECT_EQ(0, l.count());
}

TEST(TestLog, append_is_not_empty)
{
    raft::LogCommitter l(nullptr);
    l.entry_append(Entry(0, 1, UserData()));
    EXPECT_EQ(1, l.count());
}

TEST(TestLog, get_at_idx)
{
    raft::LogCommitter l(nullptr);
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.get_at_idx(2).isSome());
    EXPECT_EQ(e2.id(), l.get_at_idx(2).unwrap().id());
}

TEST(TestLog, get_at_idx_returns_null_where_out_of_bounds)
{
    raft::LogCommitter l(nullptr);

    l.entry_append(Entry(0, 1, UserData()));
    EXPECT_FALSE(l.get_at_idx(2).isSome());
}

TEST(TestLog, append_entry_user_can_set_data_buf)
{
    raft::LogCommitter l(nullptr);

    Entry ety(1, 100, UserData("aaa", 4));
    l.entry_append(ety);
    bmcl::Option<const Entry&> kept = l.get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_TRUE(kept.unwrap().isUser());
    EXPECT_EQ(ety.getUserData()->data, kept.unwrap().getUserData()->data);
}


TEST(TestLog, entry_append_increases_logidx)
{
    LogCommitter lc(nullptr);
    EXPECT_EQ(0, lc.get_current_idx());
    lc.entry_append(Entry(1, 1, UserData("aaa", 4)));
    EXPECT_EQ(1, lc.get_current_idx());
}

TEST(TestLog, entry_is_retrieveable_using_idx)
{
    std::vector<uint8_t> str = { 1, 1, 1, 1 };
    std::vector<uint8_t> str2 = { 2, 2, 2 };

    LogCommitter lc(nullptr);

    lc.entry_append(Entry(1, 1, UserData(str)));

    /* different ID so we can be successful */
    lc.entry_append(Entry(1, 2, UserData(str2)));

    bmcl::Option<const Entry&> ety_appended = lc.get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_TRUE(ety_appended.unwrap().isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, str2);
}


TEST(TestLog, idx_starts_at_1)
{
    LogCommitter lc(nullptr);
    EXPECT_EQ(0, lc.get_current_idx());

    lc.entry_append(Entry(1, 1, UserData("aaa", 4)));
    EXPECT_EQ(1, lc.get_current_idx());
}

class TestSaver : public Saver
{
public:
    std::deque<Entry> queue;
    void pop_back(const Entry& entry, std::size_t entry_idx) override
    {
        queue.push_back(entry);
    }
};

TEST(TestLog, delete)
{
    TestSaver saver;
    raft::LogCommitter l(&saver);
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    l.entry_pop_back();

    EXPECT_EQ(saver.queue.front().id(), e3.id());

    EXPECT_EQ(2, l.count());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
    l.entry_pop_back();
    EXPECT_EQ(1, l.count());
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    l.entry_pop_back();
    EXPECT_EQ(0, l.count());
    EXPECT_FALSE(l.get_at_idx(1).isSome());
}

TEST(TestLog, delete_onwards)
{
    TestSaver saver;
    raft::LogCommitter l(&saver);
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());

    /* even 3 gets deleted */
    l.entry_pop_back();
    l.entry_pop_back();
    EXPECT_EQ(1, l.count());
    EXPECT_TRUE(l.get_at_idx(1).isSome());
    EXPECT_EQ(e1.id(), l.get_at_idx(1).unwrap().id());
    EXPECT_FALSE(l.get_at_idx(2).isSome());
    EXPECT_FALSE(l.get_at_idx(3).isSome());
}

TEST(TestLog, peektail)
{
    raft::LogCommitter l(nullptr);
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    l.entry_append(e1);
    l.entry_append(e2);
    l.entry_append(e3);
    EXPECT_EQ(3, l.count());
    EXPECT_TRUE(l.back().isSome());
    EXPECT_EQ(e3.id(), l.back().unwrap().id());
}

TEST(TestLog, cant_append_duplicates)
{
    raft::LogCommitter l(nullptr);
    l.entry_append(raft::Entry(1, 1, UserData()));
    EXPECT_EQ(1, l.count());
    l.entry_append(raft::Entry(1, 1, UserData()));
    EXPECT_EQ(1, l.count());
}

TEST(TestLogCommitter, wont_apply_entry_if_we_dont_have_entry_to_apply)
{
    raft::LogCommitter lc(nullptr);
    lc.entry_apply_one();
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}

TEST(TestLogCommitter, wont_apply_entry_if_there_isnt_a_majority)
{
    raft::LogCommitter lc(nullptr);

    auto e = lc.entry_apply_one();
    EXPECT_EQ(e.unwrapErr(), Error::NothingToApply);
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());

    lc.entry_append(Entry(1, 1, raft::UserData("aaa", 4)));
    lc.entry_apply_one();
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}