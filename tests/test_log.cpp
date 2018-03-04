#include <deque>
#include <gtest/gtest.h>
#include "raft/Committer.h"
#include "mock_send_functions.h"

using namespace raft;

TEST(TestMemStorage, new_is_empty)
{
    MemStorage s;
    EXPECT_EQ(0, s.count());
}

TEST(TestMemStorage, append_is_not_empty)
{
    MemStorage s;
    s.push_back(Entry(0, 1, UserData()));
    EXPECT_EQ(1, s.count());
}

TEST(TestMemStorage, get_at_idx)
{
    MemStorage s;
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    s.push_back(e1);
    s.push_back(e2);
    s.push_back(e3);
    EXPECT_EQ(3, s.count());

    EXPECT_EQ(3, s.count());
    EXPECT_TRUE(s.get_at_idx(2).isSome());
    EXPECT_EQ(e2.id(), s.get_at_idx(2).unwrap().id());
}

TEST(TestMemStorage, get_at_idx_returns_null_where_out_of_bounds)
{
    MemStorage s;
    s.push_back(Entry(0, 1, UserData()));
    EXPECT_FALSE(s.get_at_idx(2).isSome());
}

TEST(TestMemStorage, append_entry_user_can_set_data_buf)
{
    MemStorage s;

    Entry ety(1, 100, UserData("aaa", 4));
    s.push_back(ety);
    bmcl::Option<const Entry&> kept = s.get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_TRUE(kept.unwrap().isUser());
    EXPECT_EQ(ety.getUserData()->data, kept.unwrap().getUserData()->data);
}


TEST(TestMemStorage, entry_append_increases_logidx)
{
    MemStorage s;
    EXPECT_EQ(0, s.get_current_idx());
    s.push_back(Entry(1, 1, UserData("aaa", 4)));
    EXPECT_EQ(1, s.get_current_idx());
}

TEST(TestMemStorage, entry_is_retrieveable_using_idx)
{
    std::vector<uint8_t> str = { 1, 1, 1, 1 };
    std::vector<uint8_t> str2 = { 2, 2, 2 };

    MemStorage s;

    s.push_back(Entry(1, 1, UserData(str)));

    /* different ID so we can be successful */
    s.push_back(Entry(1, 2, UserData(str2)));

    bmcl::Option<const Entry&> ety_appended = s.get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_TRUE(ety_appended.unwrap().isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, str2);
}

TEST(TestMemStorage, idx_starts_at_1)
{
    MemStorage s;
    EXPECT_EQ(0, s.get_current_idx());

    s.push_back(Entry(1, 1, UserData("aaa", 4)));
    EXPECT_EQ(1, s.get_current_idx());
}

TEST(TestMemStorage, delete)
{
    MemStorage s;
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    s.push_back(e1);
    s.push_back(e2);
    s.push_back(e3);

    EXPECT_EQ(3, s.count());
    EXPECT_EQ(s.get_at_idx(1)->id(), e1.id());
    EXPECT_EQ(s.get_at_idx(2)->id(), e2.id());
    EXPECT_EQ(s.get_at_idx(3)->id(), e3.id());
    EXPECT_FALSE(s.get_at_idx(4).isSome());


    s.pop_back();
    EXPECT_EQ(2, s.count());
    EXPECT_EQ(s.get_at_idx(1)->id(), e1.id());
    EXPECT_EQ(s.get_at_idx(2)->id(), e2.id());
    EXPECT_FALSE(s.get_at_idx(3).isSome());

    s.pop_back();
    EXPECT_EQ(1, s.count());
    EXPECT_EQ(s.get_at_idx(1)->id(), e1.id());
    EXPECT_FALSE(s.get_at_idx(2).isSome());

    s.pop_back();
    EXPECT_EQ(0, s.count());
    EXPECT_FALSE(s.get_at_idx(1).isSome());
}

TEST(TestMemStorage, delete_onwards)
{
    MemStorage s;
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    s.push_back(e1);
    s.push_back(e2);
    s.push_back(e3);
    EXPECT_EQ(3, s.count());

    /* even 3 gets deleted */
    s.pop_back();
    s.pop_back();
    EXPECT_EQ(1, s.count());
    EXPECT_TRUE(s.get_at_idx(1).isSome());
    EXPECT_EQ(e1.id(), s.get_at_idx(1).unwrap().id());
    EXPECT_FALSE(s.get_at_idx(2).isSome());
    EXPECT_FALSE(s.get_at_idx(3).isSome());
}

TEST(TestMemStorage, peektail)
{
    MemStorage s;
    Entry e1(0, 1, UserData()), e2(0, 2, UserData()), e3(0, 3, UserData());

    s.push_back(e1);
    s.push_back(e2);
    s.push_back(e3);
    EXPECT_EQ(3, s.count());
    EXPECT_TRUE(s.back().isSome());
    EXPECT_EQ(e3.id(), s.back().unwrap().id());
}

TEST(TestMemStorage, cant_append_duplicates)
{
    MemStorage s;
    s.push_back(raft::Entry(1, 1, UserData()));
    EXPECT_EQ(1, s.count());
    s.push_back(raft::Entry(1, 1, UserData()));
    EXPECT_EQ(1, s.count());
}

TEST(TestServer, apply_entry_increments_last_applied_idx)
{
    Saver saver;
    MemStorage storage;
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));

    Committer lc(&storage);
    lc.set_commit_idx(1);
    lc.entry_apply_one(&saver);
    EXPECT_EQ(1, lc.get_last_applied_idx());
}

TEST(TestLogCommitter, wont_apply_entry_if_we_dont_have_entry_to_apply)
{
    Saver saver;
    MemStorage storage;
    Committer lc(&storage);

    lc.entry_apply_one(&saver);
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}

TEST(TestLogCommitter, wont_apply_entry_if_there_isnt_a_majority)
{
    Saver saver;
    MemStorage s;
    Committer lc(&s);

    auto e = lc.entry_apply_one(&saver);
    EXPECT_EQ(e.unwrapErr(), Error::NothingToApply);
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());

    s.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    lc.entry_apply_one(&saver);
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    EXPECT_EQ(0, lc.get_last_applied_idx());
    EXPECT_EQ(0, lc.get_commit_idx());
}