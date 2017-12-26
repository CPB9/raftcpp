#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"
// TODO: leader doesn't timeout and cause election

using namespace raft;

static void prepare_follower(raft::Server& r)
{
    EXPECT_GT(r.nodes().count(), 1);
    raft::NodeId leader;
    if (!r.nodes().is_me(r.nodes().items().front().get_id()))
        leader = r.nodes().items().front().get_id();
    else
        leader = r.nodes().items().back().get_id();

    r.accept_req(leader, raft::MsgVoteReq(r.get_current_term() + 1, 0, 0));
    EXPECT_TRUE(r.is_follower());
}

static void prepare_candidate(raft::Server& r)
{
    EXPECT_GT(r.nodes().count(), 1);
    r.raft_periodic(r.timer().get_max_election_timeout());
    EXPECT_TRUE(r.is_candidate());
}

static void prepare_leader(raft::Server& r)
{
    if (r.nodes().get_num_voting_nodes() == 1)
    {
        r.raft_periodic(r.timer().get_max_election_timeout());
        EXPECT_TRUE(r.is_leader());
        return;
    }

    prepare_candidate(r);
    for (const auto& i : r.nodes().items())
    {
        r.nodes().get_node(i.get_id())->vote_for_me(true);
    }

    raft::NodeId last_voter;
    if (r.nodes().is_me(r.nodes().items().front().get_id()))
        last_voter = r.nodes().items().front().get_id();
    else
        last_voter = r.nodes().items().back().get_id();

    r.accept_rep(last_voter, raft::MsgVoteRep(r.get_current_term(), true));

    EXPECT_TRUE(r.is_leader());
}

class DefualtSender : public ISender
{
public:
    bmcl::Option<Error> request_vote(const NodeId& node, const MsgVoteReq& msg) override { return bmcl::None; }
    bmcl::Option<Error> append_entries(const NodeId& node, const MsgAppendEntriesReq& msg) override { return bmcl::None; }
};

DefualtSender __Sender;
Saver __Saver;

ISaver* generic_funcs() { return &__Saver; };

TEST(TestNode, get_my_node)
{
    raft::Nodes ns(raft::NodeId(1), true);
    ns.add_node(raft::NodeId(2), true);
    EXPECT_EQ(raft::NodeId(1), ns.get_my_id());
    EXPECT_NE(raft::NodeId(2), ns.get_my_id());
    EXPECT_TRUE(ns.is_me(raft::NodeId(1)));
    EXPECT_FALSE(ns.is_me(raft::NodeId(2)));
}

TEST(TestLog, idx_starts_at_1)
{
    raft::LogCommitter lc(&__Saver);
    EXPECT_EQ(0, lc.get_current_idx());

    lc.entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_EQ(1, lc.get_current_idx());
}

TEST(TestServer, currentterm_defaults_to_0)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, voting_results_in_voting)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_follower(r);
    r.accept_req(raft::NodeId(2), raft::MsgVoteReq(r.get_current_term() + 1, 0, 0));
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());

    prepare_follower(r);
    r.accept_req(raft::NodeId(3), raft::MsgVoteReq(r.get_current_term() + 1, 0, 0));
    EXPECT_EQ(raft::NodeId(3), r.get_voted_for());
}

TEST(TestServer, become_candidate_increments_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    raft::TermId term = r.get_current_term();
    prepare_candidate(r);
    EXPECT_EQ(term + 1, r.get_current_term());
}

TEST(TestServer, set_state)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    EXPECT_EQ(State::Leader, r.get_state());
}

TEST(TestServer, starts_as_follower)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    EXPECT_EQ(State::Follower, r.get_state());
}

TEST(TestServer, starts_with_election_timeout_of_1000ms)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    EXPECT_EQ(std::chrono::milliseconds(1000), r.timer().get_election_timeout());
}

TEST(TestServer, starts_with_request_timeout_of_200ms)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    EXPECT_EQ(std::chrono::milliseconds(200), r.timer().get_request_timeout());
}

TEST(TestLog, entry_append_increases_logidx)
{
    raft::LogCommitter lc(&__Saver);
    EXPECT_EQ(0, lc.get_current_idx());
    lc.entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_EQ(1, lc.get_current_idx());
}

TEST(TestServer, append_entry_means_entry_gets_current_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);
    prepare_follower(r);
    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 2);

    EXPECT_EQ(0, r.log().get_current_idx());
    r.accept_entry(raft::MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_EQ(1, r.log().get_current_idx());
    EXPECT_EQ(r.get_current_term(), r.log().back()->term);
}

TEST(TestLog, append_entry_is_retrievable)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    prepare_follower(r);
    prepare_leader(r);

    LogEntry ety(1, 100, raft::LogEntryData("aaa", 4));
    r.accept_entry(ety);

    bmcl::Option<const LogEntry&> kept = r.log().get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_FALSE(kept.unwrap().data.data.empty());
    EXPECT_EQ(ety.data.data, kept.unwrap().data.data);
    EXPECT_EQ(kept.unwrap().term, r.get_current_term());
}

TEST(TestLog, append_entry_user_can_set_data_buf)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    LogEntry ety(1, 100, raft::LogEntryData("aaa", 4));
    r.log().entry_append(ety);
    bmcl::Option<const LogEntry&> kept = r.log().get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_EQ(ety.data.data, kept.unwrap().data.data);
}

TEST(TestLog, entry_is_retrieveable_using_idx)
{
    std::vector<uint8_t> str = {1, 1, 1, 1};
    std::vector<uint8_t> str2 = {2, 2, 2};

    raft::LogCommitter lc(&__Saver);

    lc.entry_append(LogEntry(1, 1, raft::LogEntryData(str)));

    /* different ID so we can be successful */
    lc.entry_append(LogEntry(1, 2, raft::LogEntryData(str2)));

    bmcl::Option<const LogEntry&> ety_appended = lc.get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, str2);
}


/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (§5.3) */
TEST(TestServer, increment_lastApplied_when_lastApplied_lt_commitidx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_follower(r);

    /* need at least one entry */
    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().set_commit_idx(1);

    /* let time lapse */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestServer, apply_entry_increments_last_applied_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);

    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().set_commit_idx(1);
    r.log().entry_apply_one();
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestServer, periodic_elapses_election_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    /* we don't want to set the timeout to zero */
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(0));
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(100));
    EXPECT_EQ(100, r.timer().get_timeout_elapsed().count());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.is_leader());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node)
{
    raft::Server r(raft::NodeId(1), false, &__Sender, &__Saver);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.is_leader());
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_not_start_election_if_there_are_no_voting_nodes)
{
    raft::Server r(raft::NodeId(1), false, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), false);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_node)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), false);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, recv_entry_auto_commits_if_we_are_the_only_node)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), false);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    r.raft_periodic(r.timer().get_election_timeout());
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* receive entry */
    auto cr = r.accept_entry(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());
    EXPECT_EQ(1, r.log().get_commit_idx());
}

TEST(TestServer, recv_entry_fails_if_there_is_already_a_voting_change)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), false);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    r.raft_periodic(r.timer().get_election_timeout());
    EXPECT_TRUE(r.is_leader());
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* entry message */
    MsgAddEntryReq ety(0, 1, LotType::AddNode, raft::NodeId(1), raft::LogEntryData("aaa", 4));

    /* receive entry */
    EXPECT_TRUE(r.accept_entry(ety).isOk());
    EXPECT_EQ(1, r.log().count());

    ety.id = 2;
    auto cr = r.accept_entry(ety);
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(raft::Error::OneVotingChangeOnly, cr.unwrapErr());
    EXPECT_EQ(1, r.log().get_commit_idx());
}

/* If term > currentTerm, set currentTerm to term (step down if candidate or
 * leader) */
TEST(TestServer, votes_are_majority_is_true)
{
    /* 1 of 3 = lose */
    EXPECT_FALSE(raft::Nodes::votes_has_majority(3, 1));

    /* 2 of 3 = win */
    EXPECT_TRUE(raft::Nodes::votes_has_majority(3, 2));

    /* 2 of 5 = lose */
    EXPECT_FALSE(raft::Nodes::votes_has_majority(5, 2));

    /* 3 of 5 = win */
    EXPECT_TRUE(raft::Nodes::votes_has_majority(5, 3));

    /* 2 of 1?? This is an error */
    EXPECT_FALSE(raft::Nodes::votes_has_majority(1, 2));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_not_granted)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgVoteRep(r.get_current_term(), false));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    r.accept_rep(raft::NodeId(2), MsgVoteRep(2, true));
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_increase_votes_for_me)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    prepare_candidate(r);
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgVoteRep(r.get_current_term(), true));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_must_be_candidate_to_receive)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    for (const auto& i : r.nodes().items())
        r.nodes().get_node(i.get_id())->vote_for_me(false);
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, true));
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

/* Reply false if term < currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_false_if_term_less_than_current_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /* term is less than current term */
    MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() - 1, 0, 0));
    EXPECT_FALSE(rvr.vote_granted);
}

TEST(TestServer, leader_recv_requestvote_does_not_step_down)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);

    /* term is less than current term */
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() - 1, 0, 0));
    EXPECT_EQ(raft::NodeId(1), r.get_current_leader());
}

/* Reply true if term >= currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /* term is less than current term */
    MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, 0));
    EXPECT_TRUE(rvr.vote_granted);
}

TEST(TestServer, recv_requestvote_reset_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    r.raft_periodic(std::chrono::milliseconds(900));

    MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, 0));
    EXPECT_TRUE(rvr.vote_granted);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

TEST(TestServer, recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    /* current term is less than term */
    r.accept_req(raft::NodeId(2), MsgVoteReq(2, 1, 0));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());
}

TEST(TestServer, recv_requestvote_depends_on_candidate_id)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    /* current term is less than term */
    MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, 0));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (§5.2, §5.4) */
TEST(TestServer, recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    /* vote for self */
    prepare_candidate(r);
    EXPECT_TRUE(r.is_candidate());
    EXPECT_EQ(r.get_voted_for(), raft::NodeId(1));
    {
        MsgVoteRep rvr = r.accept_req(raft::NodeId(3), MsgVoteReq(r.get_current_term(), 1, 1));
        EXPECT_FALSE(rvr.vote_granted);
    }

    /* vote for ID 2 */
    prepare_follower(r);
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, 1));
    EXPECT_EQ(r.get_voted_for(), raft::NodeId(2));
    {
        MsgVoteRep rvr = r.accept_req(raft::NodeId(3), MsgVoteReq(r.get_current_term(), 1, 1));
        EXPECT_FALSE(rvr.vote_granted);
    }
}

TEST(TestFollower, becomes_follower_is_follower)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);
    EXPECT_TRUE(r.is_follower());
}

TEST(TestFollower, becomes_follower_does_not_clear_voted_for)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    raft::MsgAppendEntriesReq ae{ r.get_current_term(), r.log().get_current_idx(), r.log().get_last_log_term().unwrapOr(0), 0 };
    r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(r.is_follower());

    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

/* 5.1 */
TEST(TestFollower, recv_appendentries_reply_false_if_term_less_than_currentterm)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    /* no leader known at this point */
    EXPECT_FALSE(r.get_current_leader().isSome());

    /* term is low */
    MsgAppendEntriesReq ae(1);

    /*  higher current term */
    prepare_follower(r);
    prepare_follower(r);
    EXPECT_GT(r.get_current_term(), 1);

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_FALSE(aer.unwrap().success);
    /* rejected appendentries doesn't change the current leader. */
    EXPECT_FALSE(r.get_current_leader().isSome());
}

TEST(TestFollower, recv_appendentries_from_unknown_node_fails)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    auto aer = r.accept_req(raft::NodeId(3), MsgAppendEntriesReq(1));
    EXPECT_FALSE(aer.isOk());
}

/* TODO: check if test case is needed */
TEST(TestFollower, recv_appendentries_updates_currentterm_if_term_gt_currentterm)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /*  older currentterm */
    EXPECT_FALSE(r.get_current_leader().isSome());
    raft::TermId term = r.get_current_term() + 1;

    /*  newer term for appendentry */
    /* no prev log idx */
    MsgAppendEntriesReq ae(term, 0, 0, 0);

    /*  appendentry has newer term, so we change our currentterm */
    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(term, aer.unwrap().term);
    /* term has been updated */
    EXPECT_EQ(term, r.get_current_term());
    /* and leader has been updated */
    EXPECT_EQ(raft::NodeId(2), r.get_current_leader());
}

TEST(TestFollower, recv_appendentries_does_not_log_if_no_entries_are_specified)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /*  log size s */
    EXPECT_EQ(0, r.log().count());

    /* receive an appendentry with commit */
    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 4, 1, 5));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.log().count());
}

TEST(TestFollower, recv_appendentries_increases_log)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /*  log size s */
    EXPECT_EQ(0, r.log().count());

    /* receive an appendentry with commit */
    /* first appendentries msg */
    MsgAppendEntriesReq ae(3, 0, 1, 5);
    /* include one entry */
    MsgAddEntryReq ety(2, 1, raft::LogEntryData("aaa", 4));
    /* check that old terms are passed onto the log */
    ae.entries = &ety;
    ae.n_entries = 1;

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().count());
    bmcl::Option<const LogEntry&> log = r.log().get_at_idx(1);
    EXPECT_TRUE(log.isSome());
    EXPECT_EQ(2, log.unwrap().term);
}

/*  5.3 */
TEST(TestFollower, recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    /* term is different from appendentries */
    prepare_follower(r);
    // TODO at log manually?

    /* log idx that server doesn't have */
    /* prev_log_term is less than current term (ie. 2) */
    MsgAppendEntriesReq ae(r.get_current_term() + 1, 1, r.get_current_term(), 0);
    /* include one entry */
    MsgAddEntryReq ety(r.get_current_term()-1, 1, raft::LogEntryData("aaa", 4));
    ae.entries = &ety;
    ae.n_entries = 1;

    /* trigger reply */
    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());

    /* reply is false */
    EXPECT_FALSE(aer.unwrap().success);
}

static void __create_mock_entries_for_conflict_tests(raft::LogCommitter* lc, std::vector<uint8_t>* strs)
{
    bmcl::Option<const LogEntry&> ety_appended;

    /* increase log size */
    lc->entry_append(LogEntry(1, 1, raft::LogEntryData(strs[0])));
    EXPECT_EQ(1, lc->count());

    /* this log will be overwritten by a later appendentries */
    lc->entry_append(LogEntry(1, 2, raft::LogEntryData(strs[1])));
    EXPECT_EQ(2, lc->count());
    ety_appended = lc->get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, strs[1]);

    /* this log will be overwritten by a later appendentries */
    lc->entry_append(LogEntry(1, 3, raft::LogEntryData(strs[2])));
    EXPECT_EQ(3, lc->count());
    ety_appended = lc->get_at_idx(3);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, strs[2]);
}

/* 5.3 */
TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    std::vector<uint8_t> strs[] = { {1, 1, 1}, {2, 2, 2}, {3, 3, 3} };
    __create_mock_entries_for_conflict_tests(&r.log(), strs);

    /* pass a appendentry that is newer  */

    /* entries from 2 onwards will be overwritten by this appendentries message */
    MsgAppendEntriesReq ae(r.get_current_term() + 1, 1, 1, 0);
    /* include one entry */
    std::vector<uint8_t> str4 = {4, 4, 4};
    MsgAddEntryReq mety = MsgAddEntryReq(r.get_current_term() - 1, 4, LogEntryData(str4));
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
    /* str1 is still there */

    bmcl::Option<const LogEntry&> ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, strs[0]);
    /* str4 has overwritten the last 2 entries */

    ety_appended = r.log().get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, str4);
}

TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    std::vector<uint8_t> strs[] = { { 1, 1, 1 },{ 2, 2, 2 },{ 3, 3, 3 } };
    __create_mock_entries_for_conflict_tests(&r.log(), strs);

    /* pass a appendentry that is newer  */

    /* ALL append entries will be overwritten by this appendentries message */
    MsgAppendEntriesReq ae(2, 0, 0, 0);
    /* include one entry */
    std::vector<uint8_t> str4 = { 4, 4, 4 };
    MsgAddEntryReq mety = LogEntry(0, 4, LogEntryData(str4));
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().count());
    /* str1 is gone */
    bmcl::Option<const LogEntry&> ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, str4);
}

TEST(TestFollower, recv_appendentries_delete_entries_if_current_idx_greater_than_prev_log_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    std::vector<uint8_t> strs[] = { { 1, 1, 1 },{ 2, 2, 2 },{ 3, 3, 3 } };
    __create_mock_entries_for_conflict_tests(&r.log(), strs);
    EXPECT_EQ(3, r.log().count());

    MsgAppendEntriesReq ae(r.get_current_term() + 1, 1, 1, 0);
    MsgAddEntryReq e(0, 1);
    ae.entries = &e;
    ae.n_entries = 1;

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
    bmcl::Option<const LogEntry&> ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(ety_appended.unwrap().data.data, strs[0]);
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

TEST(TestFollower, recv_appendentries_add_new_entries_not_already_in_log)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    MsgAppendEntriesReq ae(r.get_current_term(), 0, 1, 0);
    /* include entries */
    MsgAddEntryReq e[2] = {LogEntry(0, 1), LogEntry(0, 2) };
    ae.entries = e;
    ae.n_entries = 2;

    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
}

TEST(TestFollower, recv_appendentries_does_not_add_dupe_entries_already_in_log)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    MsgAppendEntriesReq ae(r.get_current_term(), 0, 1, 0);
    /* include 1 entry */
    MsgAddEntryReq e[2] = { LogEntry(0, 1), LogEntry(0, 2) };
    ae.entries = e;
    ae.n_entries = 1;

    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* still successful even when no raft_append_entry() happened! */
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
        EXPECT_EQ(1, r.log().count());
    }

    /* lets get the server to append 2 now! */
    ae.n_entries = 2;
    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
TEST(TestFollower, recv_appendentries_set_commitidx_to_prevLogIdx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    MsgAppendEntriesReq ae(1, 0, 1, 0);
    /* include entries */
    MsgAddEntryReq e[4] = {LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3), LogEntry(1, 4)};
    ae.entries = e;
    ae.n_entries = 4;

    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    ae = MsgAppendEntriesReq(1, 4, 1, 5);

    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 4 because commitIDX is lower */
    EXPECT_EQ(4, r.log().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_set_commitidx_to_LeaderCommit)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    MsgAppendEntriesReq ae(1, 0, 1, 0);
    /* include entries */
    MsgAddEntryReq e[4] = { LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3), LogEntry(1, 4) };
    ae.entries = e;
    ae.n_entries = 4;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 3, 1, 3));
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 3 because leaderCommit is lower */
    EXPECT_EQ(3, r.log().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_failure_includes_current_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    r.log().entry_append(LogEntry(r.get_current_term(), 1, raft::LogEntryData("aaa", 4)));

    /* receive an appendentry with commit */
    /* lower term means failure */
    MsgAppendEntriesReq ae(r.get_current_term()-1, 0, 0, 0);
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_FALSE(aer.unwrap().success);
        EXPECT_EQ(1, aer.unwrap().current_idx);
    }

    /* try again with a higher current_idx */
    r.log().entry_append(LogEntry(r.get_current_term(), 2, raft::LogEntryData("aaa", 4)));
    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_FALSE(aer.unwrap().success);
    EXPECT_EQ(2, aer.unwrap().current_idx);
}

TEST(TestFollower, becomes_candidate_when_election_timeout_occurs)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    /*  1 second election timeout */
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /*  max election timeout have passed */
    r.raft_periodic(r.timer().get_max_election_timeout() + std::chrono::milliseconds(1));

    /* is a candidate now */
    EXPECT_TRUE(r.is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, dont_grant_vote_if_candidate_has_a_less_complete_log)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */

    /* server's idx are more up-to-date */
    raft::TermId term = r.get_current_term();
    r.log().entry_append(LogEntry(term, 100, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(term + 1, 101, raft::LogEntryData("aaa", 4)));

    /* vote not granted */
    {
        MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(term, 1, 1));
        EXPECT_FALSE(rvr.vote_granted);
    }

    /* approve vote, because last_log_term is higher */
    prepare_follower(r);
    {
        MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term(), 1, r.get_current_term() + 1));
        EXPECT_TRUE(rvr.vote_granted);
    }
}

TEST(TestFollower, recv_appendentries_heartbeat_does_not_overwrite_logs)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    MsgAppendEntriesReq ae(1, 0, 1, 0);
    /* include entries */
    MsgAddEntryReq e1(1, 1);
    ae.entries = &e1;
    ae.n_entries = 1;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE
     * NOTE: the server has received a response from the last AE so
     * prev_log_idx has been incremented */
    ae = MsgAppendEntriesReq(1, 1, 1, 0);
    /* include entries */
    MsgAddEntryReq e2[4] = { MsgAddEntryReq(1, 2), MsgAddEntryReq(1, 3), MsgAddEntryReq(1, 4), MsgAddEntryReq(1, 5) };
    ae.entries = e2;
    ae.n_entries = 4;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* receive a heartbeat
     * NOTE: the leader hasn't received the response to the last AE so it can 
     * only assume prev_Log_idx is still 1 */
    ae = MsgAppendEntriesReq(1, 1, 1, 0);
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    EXPECT_EQ(5, r.log().get_current_idx());
}

TEST(TestFollower, recv_appendentries_does_not_deleted_commited_entries)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    MsgAppendEntriesReq ae(1, 0, 1, 0);
    /* include entries */
    MsgAddEntryReq e[7] = {LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3), LogEntry(1, 4), LogEntry(1, 5), LogEntry(1, 6), LogEntry(1, 7) };
    ae.entries = &e[0];
    ae.n_entries = 1;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* Follow up AE. Node responded with success */
    ae = MsgAppendEntriesReq(1, 1, 1, 4);
    /* include entries */
    ae.entries = &e[1];
    ae.n_entries = 4;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE */
    ae = MsgAppendEntriesReq(1, 1, 1, 4);
    /* include entries */
    ae.entries = &e[1];
    ae.n_entries = 5;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.log().get_current_idx());
    EXPECT_EQ(4, r.log().get_commit_idx());

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    ae = MsgAppendEntriesReq(1, 3, 1, 4);
    /* include entries */
    ae.entries = &e[4];
    ae.n_entries = 3;
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.log().get_current_idx());
}

TEST(TestCandidate, becomes_candidate_is_candidate)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_candidate(r);
    EXPECT_TRUE(r.is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_increments_current_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    EXPECT_EQ(0, r.get_current_term());
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_votes_for_self)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    EXPECT_FALSE(r.get_voted_for().isSome());
    prepare_candidate(r);
    EXPECT_EQ(r.nodes().get_my_id(), r.get_voted_for());
    EXPECT_EQ(2, r.nodes().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_resets_election_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(900));
    EXPECT_EQ(900, r.timer().get_timeout_elapsed().count());

    prepare_candidate(r);
    /* time is selected randomly */
    EXPECT_LT(r.timer().get_timeout_elapsed().count(), 1000);
}

TEST(TestFollower, recv_appendentries_resets_election_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    r.raft_periodic(std::chrono::milliseconds(900));

    auto aer = r.accept_req(raft::NodeId(1), MsgAppendEntriesReq(1));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_requests_votes_from_other_servers)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    raft::Server r2(raft::NodeId(2), true, &__Sender, &__Saver);
    raft::Server r3(raft::NodeId(3), true, &__Sender, &__Saver);
    Exchanger sender(&r);
    sender.add(&r2);
    sender.add(&r3);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    /* set term so we can check it gets included in the outbound message */
    prepare_follower(r);
    prepare_follower(r);

    /* becoming candidate triggers vote requests */
    prepare_candidate(r);
    raft::TermId term = r.get_current_term();

    bmcl::Option<msg_t> msg;
    MsgVoteReq* rv;
    /* 2 nodes = 2 vote requests */
    msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(term, rv->term);

    /*  TODO: there should be more items */
    msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(term, rv->term);
}

/* Candidate 5.2 */
TEST(TestCandidate, election_timeout_and_no_leader_results_in_new_election)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);

    /* server wants to be leader, so becomes candidate */
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());

    /* clock over (i.e. max election timeout + 1) to overcome possible negative timeout, causing new election */
    r.raft_periodic(r.timer().get_max_election_timeout() + std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.get_current_term());

    /*  receiving this vote gives the server majority */
//    msg_requestvote_response_t vr = {0};
//    vr.term = 0;
//    vr.vote_granted = raft_request_vote::GRANTED;
//    raft_recv_requestvote_response(r,1,&vr);
//    EXPECT_TRUE(r.raft_is_leader());
}

/* Candidate 5.2 */
TEST(TestCandidate, receives_majority_of_votes_becomes_leader)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.nodes().add_node(raft::NodeId(4), true);
    r.nodes().add_node(raft::NodeId(5), true);
    EXPECT_EQ(5, r.nodes().count());

    /* vote for self */
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    /* a vote for us */
    /* get one vote */
    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, true));
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
    EXPECT_FALSE(r.is_leader());

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    r.accept_rep(raft::NodeId(3), MsgVoteRep(1, true));
    EXPECT_TRUE(r.is_leader());
}

/* Candidate 5.2 */
TEST(TestCandidate, will_not_respond_to_voterequest_if_it_has_already_voted)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_candidate(r);

    MsgVoteRep rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(0, 0, 0));

    /* we've vote already, so won't respond with a vote granted... */
    EXPECT_FALSE(rvr.vote_granted);
}

/* Candidate 5.2 */
TEST(TestCandidate, requestvote_includes_logidx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    raft::Server r2(raft::NodeId(2), true, &__Sender, &__Saver);
    raft::Server r3(raft::NodeId(3), true, &__Sender, &__Saver);
    Exchanger sender(&r);
    sender.add(&r2);
    sender.add(&r3);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    prepare_follower(r);

    raft::TermId term = r.get_current_term();
    /* 3 entries */
    r.log().entry_append(LogEntry(1, 100, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(1, 101, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(3, 102, raft::LogEntryData("aaa", 4)));
    prepare_candidate(r); //becoming candidate means new election term? so +1
    EXPECT_EQ(term + 1, r.get_current_term());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    MsgVoteReq* rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(3, rv->last_log_idx);
    EXPECT_EQ(r.get_current_term(), rv->term);
    EXPECT_EQ(3, rv->last_log_term);
    EXPECT_EQ(raft::NodeId(1), msg.unwrap().sender);
}

TEST(TestCandidate, recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_candidate(r);
    EXPECT_FALSE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
    raft::TermId term = r.get_current_term();

    r.accept_rep(raft::NodeId(2), MsgVoteRep(term + 1, false));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(term + 1, r.get_current_term());
    EXPECT_FALSE(r.get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_frm_leader_results_in_follower)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_candidate(r);
    EXPECT_FALSE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
    EXPECT_EQ(1, r.get_current_term());

    /* receive recent appendentries */
    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(2));
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(r.is_follower());
    /* after accepting a leader, it's available as the last known leader */
    EXPECT_EQ(raft::NodeId(2), r.get_current_leader());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_FALSE(r.get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_from_same_term_results_in_step_down)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_candidate(r);
    EXPECT_FALSE(r.is_follower());
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term(), 1, 1, 0));
    EXPECT_TRUE(aer.isOk());

    EXPECT_FALSE(r.is_candidate());

    /* The election algorithm requires that votedFor always contains the node
     * voted for in the current term (if any), which is why it is persisted.
     * By resetting that to -1 we have the following problem:
     *
     *  Node self, other1 and other2 becomes candidates
     *  Node other1 wins election
     *  Node self gets appendentries
     *  Node self resets votedFor
     *  Node self gets requestvote from other2
     *  Node self votes for Other2
    */
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

TEST(TestLeader, becomes_leader_is_leader)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), false);
    prepare_leader(r);
    EXPECT_TRUE(r.is_leader());
}

TEST(TestLeader, becomes_leader_does_not_clear_voted_for)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

TEST(TestLeader, becomes_leader_when_alone_sets_voted_for)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    prepare_leader(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

TEST(TestLeader, when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_leader(r);

    int i;
    for (i = 2; i <= 3; i++)
    {
        bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::NodeId(i));
        EXPECT_TRUE(p.isSome());
        EXPECT_EQ(r.log().get_current_idx() + 1, p->get_next_idx());
    }
}

/* 5.2 */
TEST(TestLeader, when_it_becomes_a_leader_sends_empty_appendentries)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_leader(r);

    /* receive appendentries messages for both nodes */
    bmcl::Option<msg_t> msg;
    MsgAppendEntriesReq* ae;

    msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);

    msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
TEST(TestLeader, responds_to_entry_msg_when_entry_is_committed)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);
    EXPECT_EQ(0, r.log().count());

    /* receive entry */
    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());

    /* trigger response through commit */
    r.log().entry_apply_one();
}

void TestRaft_non_leader_recv_entry_msg_fails()
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /* entry message */
    MsgAddEntryReq ety(0, 1, raft::LogEntryData("aaa", 4));

    /* receive entry */
    auto cr = r.accept_entry(ety);
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(raft::Error::NotLeader, cr.unwrapErr());
}

/* 5.3 */
TEST(TestLeader, sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    sender.clear();

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::NodeId(2));
    EXPECT_TRUE(p.isSome());
    p->set_next_idx(4);

    /* receive appendentries messages */
    r.send_appendentries(p.unwrap().get_id());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

TEST(TestLeader, sends_appendentries_with_leader_commit)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    for (std::size_t i = 0; i < 10; i++)
        r.log().entry_append(MsgAddEntryReq(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().set_commit_idx(10);
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(raft::NodeId(2));
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
        EXPECT_EQ(10, ae->leader_commit);
    }
}

TEST(TestLeader, sends_appendentries_with_prevLogIdx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    bmcl::Option<raft::Node&> n = r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(n->get_id());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
    EXPECT_EQ(0, ae->prev_log_idx);

    /* add 1 entry */
    /* receive appendentries messages */
    r.log().entry_append(MsgAddEntryReq(2, 100, raft::LogEntryData("aaa", 4)));
    n->set_next_idx(1);
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(0, ae->prev_log_idx);
        EXPECT_EQ(1, ae->n_entries);
        EXPECT_EQ(100, ae->entries[0].id);
        EXPECT_EQ(2, ae->entries[0].term);
    }

    /* set next_idx */
    /* receive appendentries messages */
    n->set_next_idx(2);
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_idx);
    }
}

TEST(TestLeader, sends_appendentries_when_node_has_next_idx_of_0)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    bmcl::Option<raft::Node&> n = r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(n->get_id());
    MsgAppendEntriesReq*  ae;
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* add an entry */
    /* receive appendentries messages */
    n->set_next_idx(1);
    r.log().entry_append(MsgAddEntryReq(1, 100, raft::LogEntryData("aaa", 4)));
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(0, ae->prev_log_idx);
    }
}

/* 5.3 */
TEST(TestLeader, retries_appendentries_with_decremented_NextIdx_log_inconsistency)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(raft::NodeId(2));
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
}

/*
 * If there exists an N such that N > commitidx, a majority
 * of matchidx[i] = N, and log[N].term == currentTerm:
 * set commitidx = N (§5.2, §5.4).  */
TEST(TestLeader, append_entry_to_log_increases_idxno)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    EXPECT_EQ(0, r.log().count());

    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.nodes().add_node(raft::NodeId(4), true);
    r.nodes().add_node(raft::NodeId(5), true);

    prepare_leader(r);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(1, 2, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(1, 3, raft::LogEntryData("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 1, 1));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(1, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */

    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 2, 2));
    EXPECT_EQ(1, r.log().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 2, 2));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(2, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.log().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.nodes().add_node(raft::NodeId(4), false);
    r.nodes().add_node(raft::NodeId(5), false);

    prepare_leader(r);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(1, r.log().get_commit_idx());
    /* leader will now have majority followers who have appended this log */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_duplicate_does_not_decrement_match_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_leader(r);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.log().entry_append(MsgAddEntryReq(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(MsgAddEntryReq(1, 2, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(MsgAddEntryReq(1, 3, raft::LogEntryData("aaa", 4)));

    /* receive msg 1 */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(1, r.nodes().get_node(raft::NodeId(2))->get_match_idx());

    /* receive msg 2 */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 2, 2));
    EXPECT_EQ(2, r.nodes().get_node(raft::NodeId(2))->get_match_idx());

    /* receive msg 1 - because of duplication ie. unreliable network */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(2, r.nodes().get_node(raft::NodeId(2))->get_match_idx());
}

TEST(TestLeader, recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.nodes().add_node(raft::NodeId(4), true);
    r.nodes().add_node(raft::NodeId(5), true);

    prepare_follower(r);
    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 1);

    /* append entries - we need two */
    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(1, 2, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(2, 3, raft::LogEntryData("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.log().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 2, 2));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 2, 2));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.log().get_last_applied_idx());

    /* THIRD entry log application */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, 3, 3));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(r.get_current_term(), true, 3, 3));
    EXPECT_EQ(3, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.log().get_last_applied_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(3, r.log().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_jumps_to_lower_next_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);
    bmcl::Option<raft::Node&> node = r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /* append entries */
    r.log().entry_append(MsgAddEntryReq(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(MsgAddEntryReq(2, 2, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(MsgAddEntryReq(3, 3, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(MsgAddEntryReq(4, 4, raft::LogEntryData("aaa", 4)));

    MsgAppendEntriesReq* ae;

    /* become leader sets next_idx to current_idx */
    prepare_leader(r);
    EXPECT_EQ(5, node->get_next_idx());

    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(node->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_rep(node->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 1, 0));
    EXPECT_EQ(2, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_term);
        EXPECT_EQ(1, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_decrements_to_lower_next_idx)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);
    bmcl::Option<raft::Node&> node = r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);

    /* append entries */
    r.log().entry_append(LogEntry(1, 1, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(2, 2, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(3, 3, raft::LogEntryData("aaa", 4)));
    r.log().entry_append(LogEntry(4, 4, raft::LogEntryData("aaa", 4)));

    MsgAppendEntriesReq* ae;

    /* become leader sets next_idx to current_idx */
    prepare_leader(r);
    EXPECT_EQ(5, node->get_next_idx());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(node->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_rep(node->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 4, 0));
    EXPECT_EQ(4, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(3, ae->prev_log_term);
        EXPECT_EQ(3, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_rep(node->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 4, 0));
    EXPECT_EQ(3, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(2, ae->prev_log_term);
        EXPECT_EQ(2, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_retry_only_if_leader)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    prepare_leader(r);
    sender.clear();
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    raft::TermId term = r.get_current_term();
    r.log().entry_append(MsgAddEntryReq(term, 1, raft::LogEntryData("aaa", 4)));

    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    EXPECT_TRUE(sender.poll_msg_data(r).isSome());
    EXPECT_TRUE(sender.poll_msg_data(r).isSome());

    prepare_follower(r);

    /* receive mock success responses */
    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(term, true, 1, 1));
    EXPECT_TRUE(e.isSome());
    EXPECT_EQ(raft::Error::NotLeader, e.unwrap());
    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_from_unknown_node_fails)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);

    prepare_leader(r);

    /* receive mock success responses */
    EXPECT_TRUE(r.accept_rep(raft::NodeId(4), MsgAppendEntriesRep(r.get_current_term(), true, 0, 0)).isSome());
}

TEST(TestLeader, recv_entry_resets_election_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.timer().set_timeout(std::chrono::milliseconds(200), 5);
    prepare_leader(r);

    r.raft_periodic(std::chrono::milliseconds(900));

    /* receive entry */
    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

TEST(TestLeader, recv_entry_is_committed_returns_0_if_not_committed)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    /* receive entry */
    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(EntryState::NotCommitted, r.log().entry_get_state(cr.unwrap()));

    r.log().set_commit_idx(1);
    EXPECT_EQ(EntryState::Committed, r.log().entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_is_committed_returns_neg_1_if_invalidated)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    /* receive entry */
    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(EntryState::NotCommitted, r.log().entry_get_state(cr.unwrap()));
    EXPECT_EQ(1, cr.unwrap().term);
    EXPECT_EQ(1, cr.unwrap().idx);
    EXPECT_EQ(1, r.log().get_current_idx());
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* append entry that invalidates entry message */
    MsgAppendEntriesReq ae(2, 0, 0, 1);

    MsgAddEntryReq e(2, 999, raft::LogEntryData("aaa", 4));
    ae.entries = &e;
    ae.n_entries = 1;
    auto aer = r.accept_req(raft::NodeId(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().get_current_idx());
    EXPECT_EQ(1, r.log().get_commit_idx());
    EXPECT_EQ(EntryState::Invalidated, r.log().entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_does_not_send_new_appendentries_to_slow_nodes)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    prepare_leader(r);
    sender.clear();

    /* make the node slow */
    r.nodes().get_node(raft::NodeId(2))->set_next_idx(1);

    /* append entries */
    r.log().entry_append(MsgAddEntryReq(1, 1, raft::LogEntryData("aaa", 4)));

    /* receive entry */
    auto cr = r.accept_entry(MsgAddEntryReq(0, 1, raft::LogEntryData("bbb", 4)));
    EXPECT_TRUE(cr.isOk());

    /* check if the slow node got sent this appendentries */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }
}

TEST(TestLeader, recv_appendentries_response_failure_does_not_set_node_nextid_to_0)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    /* append entries */
    r.log().entry_append(MsgAddEntryReq(1, 1, raft::LogEntryData("aaa", 4)));

    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));

    /* receive mock success response */
    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::NodeId(2));
    EXPECT_TRUE(p.isSome());
    r.accept_rep(p->get_id(), MsgAppendEntriesRep(1, false, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
    r.accept_rep(p->get_id(), MsgAppendEntriesRep(1, false, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_increment_idx_of_node)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::NodeId(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->get_next_idx());

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_drop_message_if_term_is_old)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    prepare_follower(r);
    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 1);

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::NodeId(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->get_next_idx());

    /* receive OLD mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term() - 1, true, 1, 1));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_steps_down_if_term_is_newer)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_follower(r);
    prepare_leader(r);

    bmcl::Option<const raft::Node&> n2 = r.nodes().get_node(raft::NodeId(2));
    EXPECT_TRUE(n2.isSome());
    EXPECT_EQ(1, n2->get_next_idx());

    /* receive NEW mock failed responses */
    r.accept_rep(raft::NodeId(2), raft::MsgAppendEntriesRep(r.get_current_term() + 1, false, 2, 0));
    EXPECT_TRUE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    /* check that node 1 considers itself the leader */
    EXPECT_TRUE(r.is_leader());
    EXPECT_EQ(raft::NodeId(1), r.get_current_leader());

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term() + 1, 6, 5, 0));
    EXPECT_TRUE(aer.isOk());

    /* after more recent appendentries from node 2, node 1 should
     * consider node 2 the leader. */
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(raft::NodeId(2), r.get_current_leader());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer_term)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    r.nodes().add_node(raft::NodeId(2), true);

    prepare_leader(r);

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term() + 1, 5, 5, 0));
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(r.is_follower());
}

TEST(TestLeader, sends_empty_appendentries_every_request_timeout)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.timer().set_timeout(std::chrono::milliseconds(500), 2);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    prepare_leader(r);

    bmcl::Option<msg_t> msg;
    MsgAppendEntriesReq* ae;

    /* receive appendentries messages for both nodes */
    {
        msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }

    /* force request timeout */
    r.raft_periodic(3*r.timer().get_request_timeout());
    {
        msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
}

/* TODO: If a server receives a request with a stale term number, it rejects the request. */
#if 0
void T_estRaft_leader_sends_appendentries_when_receive_entry_msg()
#endif

TEST(TestLeader, recv_requestvote_responds_without_granting)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);

    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.timer().set_timeout(std::chrono::milliseconds(500), 2);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    prepare_candidate(r);

    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, true));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    MsgVoteRep opt = r.accept_req(raft::NodeId(3), MsgVoteReq(1, 0, 0));
    EXPECT_FALSE(opt.vote_granted);
}

TEST(TestLeader, recv_requestvote_responds_with_granting_if_term_is_higher)
{
    raft::Server r(raft::NodeId(1), true, &__Sender, &__Saver);
    Exchanger sender(&r);
    r.nodes().add_node(raft::NodeId(2), true);
    r.nodes().add_node(raft::NodeId(3), true);
    r.timer().set_timeout(std::chrono::milliseconds(500), 2);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    prepare_candidate(r);

    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, true));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    r.accept_req(raft::NodeId(3), MsgVoteReq(2, 0, 0));
    EXPECT_TRUE(r.is_follower());
}
