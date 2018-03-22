#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Committer.h"
#include "mock_send_functions.h"

using namespace raft;

static void prepare_follower(raft::Server& r)
{
    EXPECT_GT(r.nodes().count(), 1);
    raft::NodeId leader;
    if (!r.nodes().is_me(r.nodes().items().front().get_id()))
        leader = r.nodes().items().front().get_id();
    else
        leader = r.nodes().items().back().get_id();

    auto e = r.accept_req(leader, raft::MsgAppendEntriesReq(r.get_current_term() + 1));
    EXPECT_TRUE(e.isOk());
    EXPECT_TRUE(r.is_follower());
}

static void prepare_candidate(raft::Server& r)
{
    EXPECT_GT(r.nodes().count(), 1);
    r.tick(r.timer().get_max_election_timeout());
    if (r.is_candidate())
        return;

    EXPECT_TRUE(r.is_precandidate());
    for (auto& i : r.nodes().items())
    {
        r.accept_rep(i.get_id(), MsgVoteRep(r.get_current_term(), ReqVoteState::Granted));
        if (r.is_candidate())
            return;
    }
    EXPECT_TRUE(r.is_candidate());
}

static void prepare_leader(raft::Server& r)
{
    if (r.nodes().get_num_voting_nodes() == 1)
    {
        r.tick(r.timer().get_max_election_timeout());
        EXPECT_TRUE(r.is_leader());
        EXPECT_TRUE(r.get_current_leader().isSome());
        return;
    }

    prepare_candidate(r);
    for (const auto& i : r.nodes().items())
    {
        r.accept_rep(i.get_id(), raft::MsgVoteRep(r.get_current_term(), raft::ReqVoteState::Granted));
    }

    raft::NodeId last_voter;
    if (r.nodes().is_me(r.nodes().items().front().get_id()))
        last_voter = r.nodes().items().front().get_id();
    else
        last_voter = r.nodes().items().back().get_id();

    r.accept_rep(last_voter, raft::MsgVoteRep(r.get_current_term(), raft::ReqVoteState::Granted));

    EXPECT_TRUE(r.is_leader());
    EXPECT_TRUE(r.get_current_leader().isSome());
}

class DefualtSender : public ISender
{
public:
    bmcl::Option<Error> request_vote(const NodeId& node, const MsgVoteReq& msg) override { return bmcl::None; }
    bmcl::Option<Error> append_entries(const NodeId& node, const MsgAppendEntriesReq& msg) override { return bmcl::None; }
};

DefualtSender __Sender;
Applier __Applier = [](Index entry_idx, const Entry &) {return bmcl::None; };

TEST(TestServer, currentterm_defaults_to_0)
{
    MemStorage storage;
    raft::Server r(NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, voting_results_in_voting)
{
    MemStorage storage;
    raft::Server r(NodeId(1), {NodeId(1), NodeId(2), NodeId(3)}, __Applier, &storage, &__Sender);

    prepare_follower(r);
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 0, r.get_current_term() + 1, false));
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());

    prepare_follower(r);
    r.accept_req(raft::NodeId(3), MsgVoteReq(r.get_current_term() + 1, 0, r.get_current_term() + 1, false));
    EXPECT_EQ(raft::NodeId(3), r.get_voted_for());
}

TEST(TestServer, become_candidate_increments_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), {NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);

    raft::TermId term = r.get_current_term();
    prepare_candidate(r);
    EXPECT_EQ(term + 1, r.get_current_term());
}

TEST(TestServer, set_state)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    EXPECT_EQ(State::Leader, r.get_state());
}

TEST(TestServer, the_only_node_starts_as_leader)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    EXPECT_EQ(State::Leader, r.get_state());
}

TEST(TestServer, if_not_the_only_starts_as_follower)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(State::Follower, r.get_state());
}

TEST(TestServer, starts_with_election_timeout_of_1000ms)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    EXPECT_EQ(Time(1000), r.timer().get_election_timeout());
}

TEST(TestServer, starts_with_request_timeout_of_200ms)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    EXPECT_EQ(Time(200), r.timer().get_request_timeout());
}

TEST(TestServer, append_entry_means_entry_gets_current_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    prepare_follower(r);
    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 2);

    Index ci = storage.get_current_idx();
    r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_EQ(ci + 1, storage.get_current_idx());
    EXPECT_EQ(r.get_current_term(), storage.back()->term());
}

TEST(TestServer, append_entry_is_retrievable)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    prepare_follower(r);
    prepare_leader(r);

    Index ci = storage.get_current_idx();
    raft::UserData etyData("aaa", 4);
    r.add_entry(100, etyData);

    bmcl::Option<const Entry&> kept = r.committer().get_at_idx(ci + 1);
    ASSERT_TRUE(kept.isSome());
    ASSERT_TRUE(kept->isUser());
    EXPECT_FALSE(kept->getUserData()->data.empty());
    EXPECT_EQ(etyData.data, kept->getUserData()->data);
    EXPECT_EQ(kept.unwrap().term(), r.get_current_term());
}


/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (§5.3) */
TEST(TestServer, increment_lastApplied_when_lastApplied_lt_commitidx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /* need at least one entry */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    r.accept_req(NodeId(2), MsgAppendEntriesReq(r.get_current_term(), r.committer().get_current_idx(), 1, 0));
    EXPECT_EQ(1, r.committer().get_commit_idx());
    EXPECT_EQ(0, r.committer().get_last_applied_idx());

    /* let time lapse */
    r.tick();
    EXPECT_EQ(1, r.committer().get_last_applied_idx());
}

TEST(TestServer, periodic_elapses_election_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    /* we don't want to set the timeout to zero */
    r.timer().set_timeout(Time(200), 5);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.tick(Time(0));
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.tick(Time(100));
    EXPECT_EQ(100, r.timer().get_timeout_elapsed().count());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    /* clock over (ie. 1000 + 1), causing new election */
    r.tick(r.timer().get_max_election_timeout());

    EXPECT_FALSE(r.is_leader());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), false, __Applier, &storage, &__Sender);

    /* clock over (ie. 1000 + 1), causing new election */
    r.tick(r.timer().get_max_election_timeout());

    EXPECT_FALSE(r.is_leader());
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_not_start_election_if_there_are_no_voting_nodes)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), false, __Applier, &storage, &__Sender);

    r.add_node(1, raft::NodeId(2));

    /* clock over (ie. 1000 + 1), causing new election */
    r.tick(r.timer().get_max_election_timeout());

    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);

    /* clock over (ie. 1000 + 1), causing new election */
    r.tick(r.timer().get_max_election_timeout());

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node)
{
    MemStorage storage;
    raft::Server r(NodeId(1), { NodeId(1) }, __Applier, &storage, &__Sender);

    /* clock over (ie. 1000 + 1), causing new election */
    r.tick(r.timer().get_max_election_timeout());

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, recv_entry_auto_commits_if_we_are_the_only_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);

    r.tick(r.timer().get_max_election_timeout());
    raft::Index ci = r.committer().get_commit_idx();
    raft::Index count = storage.count();

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(count + 1, storage.count());
    EXPECT_EQ(ci + 1, r.committer().get_commit_idx());
}

TEST(TestServer, recv_entry_fails_if_there_is_already_a_voting_change)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    prepare_leader(r);
    raft::Index ci = r.committer().get_commit_idx();
    raft::Index count = storage.count();

    r.add_node(99, raft::NodeId(2));
    r.accept_rep(raft::NodeId(2), raft::MsgAppendEntriesRep(r.get_current_term(), true, r.committer().get_current_idx()));
    EXPECT_TRUE(r.committer().voting_change_is_in_progress());
    EXPECT_EQ(count + 1, storage.count());

    auto cr = r.add_node(2, raft::NodeId(3));
    r.accept_rep(raft::NodeId(3), raft::MsgAppendEntriesRep(r.get_current_term(), true, r.committer().get_current_idx()));

    ASSERT_TRUE(cr.isErr());
    EXPECT_EQ(raft::Error::OneVotingChangeOnly, cr.unwrapErr());
    EXPECT_EQ(ci + 1, r.committer().get_commit_idx());
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
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgVoteRep(r.get_current_term(), raft::ReqVoteState::NotGranted));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    r.accept_rep(raft::NodeId(2), MsgVoteRep(2, raft::ReqVoteState::Granted));
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_increase_votes_for_me)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    prepare_candidate(r);
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgVoteRep(r.get_current_term(), raft::ReqVoteState::Granted));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_must_be_candidate_to_receive)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    for (const auto& i : r.nodes().items())
    {
        if(i.has_vote_for_me())
            continue;

        r.accept_rep(i.get_id(), MsgVoteRep(r.get_current_term(), raft::ReqVoteState::Granted));
        EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
    }
}

/* Reply false if term < currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_false_if_term_less_than_current_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /* term is less than current term */
    auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() - 1, 0, 0, false));
    ASSERT_TRUE(rvr.isOk());
    EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
}

TEST(TestServer, leader_recv_requestvote_does_not_step_down)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    /* term is less than current term */
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() - 1, 0, 0, false));
    EXPECT_EQ(raft::NodeId(1), r.get_current_leader());
}

/* Reply true if term >= currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /* term is less than current term */
    auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, r.get_current_term() + 1, false));
    ASSERT_TRUE(rvr.isOk());
    EXPECT_EQ(raft::ReqVoteState::Granted, rvr.unwrap().vote_granted);
}

TEST(TestServer, recv_requestvote_reset_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    r.timer().set_timeout(Time(200), 5);
    r.tick(Time(900));

    auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, r.get_current_term() + 1, false));
    ASSERT_TRUE(rvr.isOk());
    EXPECT_EQ(raft::ReqVoteState::Granted, rvr.unwrap().vote_granted);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

TEST(TestServer, recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    /* current term is less than term */
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term()+1, 1, r.get_current_term()+1, false));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());
}

TEST(TestServer, recv_requestvote_depends_on_candidate_id)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    /* current term is less than term */
    auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, r.get_current_term() + 1, false));
    EXPECT_TRUE(rvr.isOk());
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::NodeId(2), r.get_voted_for());
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (§5.2, §5.4) */
TEST(TestServer, recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);

    /* vote for self */
    prepare_candidate(r);

    EXPECT_EQ(r.get_voted_for(), raft::NodeId(1));
    {
        auto rvr = r.accept_req(raft::NodeId(3), MsgVoteReq(r.get_current_term(), 1, 1, false));
        ASSERT_TRUE(rvr.isOk());
        EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
    }

    /* vote for ID 2 */
    prepare_follower(r);
    r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term() + 1, 1, 1, false));
    EXPECT_EQ(r.get_voted_for(), raft::NodeId(2));
    {
        auto rvr = r.accept_req(raft::NodeId(3), MsgVoteReq(r.get_current_term(), 1, 1, false));
        ASSERT_TRUE(rvr.isOk());
        EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
    }
}

TEST(TestFollower, becomes_follower_is_follower)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
}

TEST(TestFollower, becomes_follower_does_not_clear_voted_for)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);

    prepare_candidate(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    r.accept_req(raft::NodeId(2), raft::MsgAppendEntriesReq(r.get_current_term(), r.committer().get_last_log_term().unwrapOr(0), 0, 0));
    EXPECT_TRUE(r.is_follower());

    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

/* 5.1 */
TEST(TestFollower, recv_appendentries_reply_false_if_term_less_than_currentterm)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);

    /*  higher current term */
    prepare_follower(r);
    prepare_follower(r);
    EXPECT_GT(r.get_current_term(), 1);
    EXPECT_TRUE(r.get_current_leader().isSome());

    NodeId leader = r.get_current_leader().unwrap();
    NodeId not_leader = leader == NodeId(2) ? NodeId(3) : NodeId(2);

     /* term is low */
    auto aer = r.accept_req(not_leader, MsgAppendEntriesReq(1));
    ASSERT_TRUE(aer.isOk());
    EXPECT_FALSE(aer.unwrap().success);
    /* rejected appendentries doesn't change the current leader. */
    ASSERT_TRUE(r.get_current_leader().isSome());
    EXPECT_EQ(leader, r.get_current_leader().unwrap());
}

/* TODO: check if test case is needed */
TEST(TestFollower, recv_appendentries_updates_currentterm_if_term_gt_currentterm)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    NodeId leader = r.get_current_leader().unwrap();
    NodeId not_leader = leader == NodeId(2) ? NodeId(3) : NodeId(2);

    /*  older currentterm */
    raft::TermId higher_term = r.get_current_term() + 1;

    /*  newer term for appendentry */
    /* no prev log idx */
    /*  appendentry has newer term, so we change our currentterm */
    auto aer = r.accept_req(not_leader, MsgAppendEntriesReq(higher_term));
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(higher_term, aer.unwrap().term);

    /* term has been updated */
    EXPECT_EQ(higher_term, r.get_current_term());
    /* and leader has been updated */
    EXPECT_EQ(not_leader, r.get_current_leader());
}

TEST(TestFollower, recv_appendentries_does_not_log_if_no_entries_are_specified)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /*  log size s */
    std::size_t count = storage.count();

    /* receive an appendentry with commit */
    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 5, 0, DataHandler(&storage, count, 0)));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(count, storage.count());
}

TEST(TestFollower, recv_appendentries_increases_log)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    std::size_t count = storage.count();

    /* receive an appendentry with commit */
    /* first appendentries msg */
    /* include one entry */
    /* check that old terms are passed onto the log */
    Entry ety(2, 1, raft::UserData("aaa", 4));
    MsgAppendEntriesReq ae(3, 1, 5, 0, DataHandler(&ety, count, 1));

    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(count + 1, storage.count());
    bmcl::Option<const Entry&> log = r.committer().get_at_idx(count + 1);
    ASSERT_TRUE(log.isSome());
    EXPECT_EQ(2, log.unwrap().term());
}

/*  5.3 */
TEST(TestFollower, recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    /* term is different from appendentries */
    prepare_follower(r);
    // TODO at log manually?

    /* log idx that server doesn't have */
    /* prev_log_term is less than current term (ie. 2) */
    Entry ety(r.get_current_term()-1, 1, raft::UserData("aaa", 4));
    MsgAppendEntriesReq ae(r.get_current_term() + 1, r.get_current_term(), 0, 0, DataHandler(&ety, 10, 1));

    /* trigger reply */
    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());

    /* reply is false */
    EXPECT_FALSE(aer.unwrap().success);
}

static void __create_mock_entries_for_conflict_tests(IStorage* lc, std::vector<uint8_t>* strs)
{
    std::size_t count = lc->count();
    bmcl::Option<const Entry&> ety_appended;

    /* increase log size */
    lc->push_back(Entry(1, 1, raft::UserData(strs[0])));
    lc->push_back(Entry(1, 2, raft::UserData(strs[1])));
    lc->push_back(Entry(1, 3, raft::UserData(strs[2])));
    EXPECT_EQ(count + 3, lc->count());

    ety_appended = lc->get_at_idx(count + 1);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended->getUserData()->data, strs[0]);

    /* this log will be overwritten by a later appendentries */
    ety_appended = lc->get_at_idx(count + 2);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended->getUserData()->data, strs[1]);

    /* this log will be overwritten by a later appendentries */
    ety_appended = lc->get_at_idx(count + 3);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended->getUserData()->data, strs[2]);
}

/* 5.3 */
TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    std::size_t count = storage.count();
    std::vector<uint8_t> strs[] = { {1, 1, 1}, {2, 2, 2}, {3, 3, 3} };
    __create_mock_entries_for_conflict_tests(&storage, strs);

    /* pass a appendentry that is newer  */

    /* entries from 2 onwards will be overwritten by this appendentries message */
    /* include one entry */
    std::vector<uint8_t> str4 = {4, 4, 4};
    Entry ety = Entry(r.get_current_term() - 1, 4, UserData(str4));
    MsgAppendEntriesReq ae(r.get_current_term() + 1, 1, 0, 0, DataHandler(&ety, count + 1, 1));

    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    ASSERT_TRUE(aer.unwrap().success);
    EXPECT_EQ(count + 2, storage.count());
    /* str1 is still there */

    bmcl::Option<const Entry&> ety_appended = r.committer().get_at_idx(count + 1);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, strs[0]);
    /* str4 has overwritten the last 2 entries */

    ety_appended = r.committer().get_at_idx(count + 2);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, str4);
}

TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    std::size_t count = storage.count();

    std::vector<uint8_t> strs[] = { { 1, 1, 1 },{ 2, 2, 2 },{ 3, 3, 3 } };
    __create_mock_entries_for_conflict_tests(&storage, strs);

    /* pass a appendentry that is newer  */

    /* ALL append entries will be overwritten by this appendentries message */
    /* include one entry */
    std::vector<uint8_t> str4 = { 4, 4, 4 };
    Entry ety = Entry(0, 4, UserData(str4));
    MsgAppendEntriesReq ae(r.get_current_term(), r.get_current_term(), 0, 0, DataHandler(&ety, count, 1));

    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(count + 1, storage.count());
    /* str1 is gone */
    bmcl::Option<const Entry&> ety_appended = r.committer().get_at_idx(count + 1);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, str4);
}

TEST(TestFollower, recv_appendentries_delete_entries_if_current_idx_greater_than_prev_log_idx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    std::size_t count = storage.count();

    std::vector<uint8_t> strs[] = { { 1, 1, 1 },{ 2, 2, 2 },{ 3, 3, 3 } };
    __create_mock_entries_for_conflict_tests(&storage, strs);
    EXPECT_EQ(count + 3, storage.count());

    Entry ety = Entry::user_empty(0, 1);
    MsgAppendEntriesReq ae(r.get_current_term()+1, r.get_current_term(), 0, 0, DataHandler(&ety, count+1, 1));

    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(count + 2, storage.count());
    bmcl::Option<const Entry&> ety_appended = r.committer().get_at_idx(count + 1);
    ASSERT_TRUE(ety_appended.isSome());
    ASSERT_TRUE(ety_appended->isUser());
    EXPECT_EQ(ety_appended.unwrap().getUserData()->data, strs[0]);
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

TEST(TestFollower, recv_appendentries_add_new_entries_not_already_in_log)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    Entry ety[2] = { Entry::user_empty(0, 1), Entry::user_empty(0, 2) };
    MsgAppendEntriesReq ae(r.get_current_term(), 0, 0, 0, DataHandler(ety, 0, 2));

    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, storage.count());
}

TEST(TestFollower, recv_appendentries_does_not_add_dupe_entries_already_in_log)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    std::size_t count = storage.count();

    Entry ety[2] = { Entry::user_empty(0, 1), Entry::user_empty(0, 2) };
    MsgAppendEntriesReq ae(r.get_current_term(), 1, 0, 0, DataHandler(ety, count, 1));

    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
        EXPECT_EQ(count + 1, storage.count());
    }

    /* still successful even when no raft_append_entry() happened! */
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
        EXPECT_EQ(count + 1, storage.count());
    }

    /* lets get the server to append 2 now! */
    MsgAppendEntriesReq ae2(r.get_current_term(), 1, 0, 0, DataHandler(ety, count, 2));

    auto aer = r.accept_req(raft::NodeId(2), ae2);
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(count + 2, storage.count());
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
TEST(TestFollower, recv_appendentries_set_commitidx_to_prevLogIdx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);


    {
        Entry e[4] = { Entry::user_empty(1, 1), Entry::user_empty(1, 2), Entry::user_empty(1, 3), Entry::user_empty(1, 4) };
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0, DataHandler(e, 0, 4)));
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 5, 0));
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 4 because commitIDX is lower */
    EXPECT_EQ(4, r.committer().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_set_commitidx_to_LeaderCommit)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    {
        Entry e[4] = { Entry::user_empty(1, 1), Entry::user_empty(1, 2), Entry::user_empty(1, 3), Entry::user_empty(1, 4) };
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0, DataHandler(e, 0, 4)));
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 3, 0));
        ASSERT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 3 because leaderCommit is lower */
    EXPECT_EQ(3, r.committer().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_failure_includes_current_idx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    storage.push_back(Entry(r.get_current_term(), 1, raft::UserData("aaa", 4)));

    /* receive an appendentry with commit */
    /* lower term means failure */
    MsgAppendEntriesReq ae(r.get_current_term()-1, 0, 0, 0);
    {
        auto aer = r.accept_req(raft::NodeId(2), ae);
        ASSERT_TRUE(aer.isOk());
        EXPECT_FALSE(aer.unwrap().success);
        EXPECT_EQ(storage.get_current_idx(), aer.unwrap().current_idx);
    }

    /* try again with a higher current_idx */
    storage.push_back(Entry(r.get_current_term(), 2, raft::UserData("aaa", 4)));
    auto aer = r.accept_req(raft::NodeId(2), ae);
    ASSERT_TRUE(aer.isOk());
    EXPECT_FALSE(aer.unwrap().success);
    EXPECT_EQ(storage.get_current_idx(), aer.unwrap().current_idx);
}

TEST(TestFollower, becomes_precandidate_when_election_timeout_occurs)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    /*  max election timeout have passed */
    r.tick(r.timer().get_max_election_timeout());

    /* is a candidate now */
    EXPECT_TRUE(r.is_precandidate());
}

/* Candidate 5.2 */
TEST(TestFollower, dont_grant_vote_if_candidate_has_a_less_complete_log)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */

    /* server's idx are more up-to-date */
    raft::TermId term = r.get_current_term();
    storage.push_back(Entry(term, 100, raft::UserData("aaa", 4)));
    storage.push_back(Entry(term + 1, 101, raft::UserData("aaa", 4)));

    /* vote not granted */
    {
        auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(term, 1, 1, false));
        ASSERT_TRUE(rvr.isOk());
        EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
    }

    /* approve vote, because last_log_term is higher */
    prepare_follower(r);
    {
        auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(r.get_current_term(), 1, r.get_current_term() + 1, false));
        ASSERT_TRUE(rvr.isOk());
        EXPECT_EQ(raft::ReqVoteState::Granted, rvr.unwrap().vote_granted);
    }
}

TEST(TestFollower, recv_appendentries_heartbeat_does_not_overwrite_logs)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    {
        Entry e1 = Entry::user_empty(1, 1);
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0, DataHandler(&e1, 0, 1)));
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE
     * NOTE: the server has received a response from the last AE so
     * prev_log_idx has been incremented */
    {
        Entry e2[4] = { Entry::user_empty(1, 2), Entry::user_empty(1, 3), Entry::user_empty(1, 4), Entry::user_empty(1, 5) };
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0, DataHandler(e2, 1, 4)));
        EXPECT_TRUE(aer.isOk());
    }

    /* receive a heartbeat
     * NOTE: the leader hasn't received the response to the last AE so it can
     * only assume prev_Log_idx is still 1 */
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0));
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    EXPECT_EQ(5, r.committer().get_current_idx());
}

TEST(TestFollower, recv_appendentries_does_not_deleted_commited_entries)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    Entry e[7] = {Entry::user_empty(1, 1), Entry::user_empty(1, 2), Entry::user_empty(1, 3), Entry::user_empty(1, 4), Entry::user_empty(1, 5), Entry::user_empty(1, 6), Entry::user_empty(1, 7) };

    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 0, 0, DataHandler(&e[0], 0, 1)));
        EXPECT_TRUE(aer.isOk());
    }

    /* Follow up AE. Node responded with success */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 4, 0, DataHandler(&e[1], 1, 4)));
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 4, 0, DataHandler(&e[1], 1, 5)));
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.committer().get_current_idx());
    EXPECT_EQ(4, r.committer().get_commit_idx());

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    {
        auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(1, 1, 4, 0, DataHandler(&e[4], 3, 3)));
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.committer().get_current_idx());
}

TEST(TestCandidate, becomes_candidate_is_candidate)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_candidate(r);
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_increments_current_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(0, r.get_current_term());
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_votes_for_self)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    EXPECT_FALSE(r.get_voted_for().isSome());
    prepare_candidate(r);
    EXPECT_EQ(r.nodes().get_my_id(), r.get_voted_for());
    EXPECT_EQ(2, r.nodes().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_resets_election_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    r.timer().set_timeout(Time(200), 5);
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    r.tick(Time(900));
    EXPECT_EQ(900, r.timer().get_timeout_elapsed().count());

    prepare_candidate(r);
    /* time is selected randomly */
    EXPECT_LT(r.timer().get_timeout_elapsed().count(), 1000);
}

TEST(TestFollower, recv_appendentries_resets_election_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    r.timer().set_timeout(Time(200), 5);

    r.tick(Time(900));

    auto aer = r.accept_req(raft::NodeId(1), MsgAppendEntriesReq(1));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_requests_votes_from_other_servers)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    raft::Server r2(raft::NodeId(2), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    raft::Server r3(raft::NodeId(3), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    Exchanger sender(&r);
    sender.add(&r2);
    sender.add(&r3);

    /* set term so we can check it gets included in the outbound message */
    prepare_follower(r);

    /* becoming candidate triggers vote requests */
    prepare_candidate(r);
    raft::TermId term = r.get_current_term();

    bmcl::Option<msg_t> msg;
    MsgVoteReq* rv;
    /* 2 nodes = 2 vote requests */
    msg = sender.poll_msg_data(r);
    ASSERT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(term, rv->term);

    /*  TODO: there should be more items */
    msg = sender.poll_msg_data(r);
    ASSERT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(term, rv->term);
}

TEST(TestFollower, remove_other_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    EXPECT_TRUE(r.get_current_leader().isSome());

    TermId t = r.get_current_term();
    NodeId leader = r.get_current_leader().unwrap();
    NodeId rem = leader == NodeId(2) ? NodeId(3) : NodeId(2);

    Entry ety = Entry::remove_node (t, 0, rem);
    auto e = r.accept_req(leader, MsgAppendEntriesReq(t, t - 1, 4, 0, DataHandler(&ety, 0, 1)));
    r.tick();
    EXPECT_TRUE(e.isOk());
    EXPECT_EQ(2, r.nodes().count());
    EXPECT_FALSE(r.nodes().get_node(rem).isSome());
}

TEST(TestFollower, remove_me)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_follower(r);
    EXPECT_TRUE(r.get_current_leader().isSome());

    TermId t = r.get_current_term();
    NodeId leader = r.get_current_leader().unwrap();
    NodeId rem = leader == NodeId(2) ? NodeId(3) : NodeId(2);

    Entry ety = Entry::remove_node (t, 0, r.nodes().get_my_id());
    auto e = r.accept_req(leader, MsgAppendEntriesReq(t, t - 1, 4, 0, DataHandler(&ety, 0, 1)));
    r.tick();
    EXPECT_TRUE(e.isOk());
    EXPECT_EQ(2, r.nodes().count());
    EXPECT_FALSE(r.nodes().get_my_node().isSome());
    EXPECT_TRUE(r.is_shutdown());
}

TEST(TestFollower, remove_than_add_again)
{
    MemStorage storage;
    Index i = 0;
    storage.push_back(Entry::add_node(1, ++i, NodeId(1)));

    storage.push_back(Entry::add_nonvoting_node(1, ++i, NodeId(2)));
    storage.push_back(Entry::add_node(1, ++i, NodeId(2)));

    storage.push_back(Entry::add_nonvoting_node(1, ++i, NodeId(3)));
    storage.push_back(Entry::add_node(1, ++i, NodeId(3)));

    storage.push_back(Entry::remove_node(1, ++i, NodeId(2)));
    storage.push_back(Entry::user_empty(1, ++i));

    storage.push_back(Entry::add_nonvoting_node(1, ++i, NodeId(2)));
    storage.push_back(Entry::user_empty(1, ++i));

    raft::Server r(NodeId(2), false, __Applier, &storage, nullptr);
    EXPECT_EQ(0, r.nodes().count());
    EXPECT_TRUE(r.is_follower());
    EXPECT_FALSE(r.is_shutdown());

    raft::MsgAppendEntriesReq req(1, 1, i, i-2);
    bmcl::Option<raft::Error> e = r.accept_req(NodeId(3), req).takeErrOption();
    EXPECT_TRUE(e.isNone());

    e = r.apply_all();

    EXPECT_TRUE(r.nodes().get_my_node().isSome());
    EXPECT_FALSE(r.committer().has_not_applied());
    EXPECT_FALSE(r.is_shutdown());
}

/* Candidate 5.2 */
TEST(TestCandidate, election_timeout_and_no_leader_results_in_new_election)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);

    /* server wants to be leader, so becomes candidate */
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());
    EXPECT_TRUE(r.is_candidate());

    /* clock over to overcome possible negative timeout, causing new pre vote phase without term change*/
    r.tick(r.timer().get_max_election_timeout());
    EXPECT_TRUE(r.is_precandidate());
    EXPECT_EQ(1, r.get_current_term());

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
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5) }, __Applier, &storage, &__Sender);
    EXPECT_EQ(5, r.nodes().count());

    /* vote for self */
    prepare_candidate(r);
    EXPECT_EQ(1, r.get_current_term());
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    /* a vote for us */
    /* get one vote */
    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, raft::ReqVoteState::Granted));
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
    EXPECT_FALSE(r.is_leader());

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    r.accept_rep(raft::NodeId(3), MsgVoteRep(1, raft::ReqVoteState::Granted));
    EXPECT_TRUE(r.is_leader());
}

/* Candidate 5.2 */
TEST(TestCandidate, will_not_respond_to_voterequest_if_it_has_already_voted)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);
    prepare_candidate(r);

    auto rvr = r.accept_req(raft::NodeId(2), MsgVoteReq(0, 0, 0, false));
    EXPECT_TRUE(rvr.isOk());

    /* we've vote already, so won't respond with a vote granted... */
    EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
}

/* Candidate 5.2 */
TEST(TestCandidate, requestvote_includes_logidx)
{
    MemStorage storage1;
    MemStorage storage2;
    MemStorage storage3;
    raft::Server r1(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage1, &__Sender);
    raft::Server r2(raft::NodeId(2), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage2, &__Sender);
    raft::Server r3(raft::NodeId(3), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage3, &__Sender);
    Exchanger sender(&r1);
    sender.add(&r2);
    sender.add(&r3);

    prepare_follower(r1);

    std::size_t count = storage1.count();
    raft::TermId term = r1.get_current_term();
    /* 3 entries */
    storage1.push_back(Entry(1, 100, raft::UserData("aaa", 4)));
    storage1.push_back(Entry(1, 101, raft::UserData("aaa", 4)));
    storage1.push_back(Entry(3, 102, raft::UserData("aaa", 4)));
    prepare_candidate(r1); //becoming candidate means new election term? so +1
    EXPECT_EQ(term + 1, r1.get_current_term());
    EXPECT_EQ(count + 3, storage1.count());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r1);
    ASSERT_TRUE(msg.isSome());
    MsgVoteReq* rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(count + 3, rv->last_log_idx);
    EXPECT_EQ(r1.get_current_term(), rv->term);
    EXPECT_EQ(3, rv->last_log_term);
    EXPECT_EQ(raft::NodeId(1), msg.unwrap().sender);
}

TEST(TestCandidate, recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_candidate(r);

    EXPECT_FALSE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
    raft::TermId term = r.get_current_term();

    r.accept_rep(raft::NodeId(2), MsgVoteRep(term + 1, raft::ReqVoteState::NotGranted));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(term + 1, r.get_current_term());
    EXPECT_FALSE(r.get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_frm_leader_results_in_follower)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);
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
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);
    prepare_candidate(r);

    EXPECT_FALSE(r.is_follower());
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term(), 1, 0, 0));
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

TEST(TestCandidate, recv_appendentries_doesnt_use_1_cfg_change_restriction)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);
    prepare_follower(r);

    raft::TermId term = r.get_current_term();
    raft::Entry entries[3] =
    { raft::Entry::add_node(term, 1, raft::NodeId(3))
    , raft::Entry::add_node(term, 2, raft::NodeId(4))
    , raft::Entry::add_node(term, 3, raft::NodeId(5))
    };

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term(), 1, 4, 0, DataHandler(entries, 0, 3)));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(3, storage.count());
    EXPECT_EQ(3, r.committer().get_commit_idx());

    r.tick();
    r.tick();
    r.tick();
    EXPECT_FALSE(r.committer().has_not_applied());
    EXPECT_EQ(5, r.nodes().count());
}

TEST(TestLeader, becomes_leader_is_leader)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
}

TEST(TestLeader, becomes_leader_does_not_clear_voted_for)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

TEST(TestLeader, becomes_leader_when_alone_sets_voted_for)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    prepare_leader(r);
    EXPECT_EQ(raft::NodeId(1), r.get_voted_for());
}

TEST(TestLeader, when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3)}, __Applier, &storage, &__Sender);
    prepare_leader(r);

    int i;
    for (i = 2; i <= 3; i++)
    {
        bmcl::Option<const raft::Node&> p = r.nodes().get_node(raft::NodeId(i));
        EXPECT_TRUE(p.isSome());
        EXPECT_EQ(r.committer().get_current_idx() + 1, p->get_next_idx());
    }
}

/* 5.2 */
TEST(TestLeader, when_it_becomes_a_leader_sends_empty_appendentries)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);

    /* receive appendentries messages for both nodes */
    bmcl::Option<msg_t> msg;

    r.tick(r.timer().get_request_timeout());
    msg = sender.poll_msg_data(r);
    {
        ASSERT_TRUE(msg.isSome());
        MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    r.tick(r.timer().get_request_timeout());
    msg = sender.poll_msg_data(r);
    {
        ASSERT_TRUE(msg.isSome());
        MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
TEST(TestLeader, responds_to_entry_msg_when_entry_is_committed)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Index count = storage.count();

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(count + 1, storage.count());
    EXPECT_EQ(0, r.committer().get_last_applied_idx());
}

TEST(TestFollower, non_leader_recv_entry_msg_fails)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_follower(r);

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(raft::Error::NotLeader, cr.unwrapErr());
}

/* 5.3 */
TEST(TestLeader, sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);
    sender.clear();

    storage.push_back(Entry(r.get_current_term(), 1, UserData()));
    storage.push_back(Entry(r.get_current_term(), 2, UserData()));
    storage.push_back(Entry(r.get_current_term(), 3, UserData()));
    storage.push_back(Entry(r.get_current_term(), 4, UserData()));

    bmcl::Option<const Node&> n = r.nodes().get_node(raft::NodeId(2));
    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), true, 3));
    EXPECT_EQ(4, n->get_next_idx());

    storage.pop_back();
    storage.pop_back();
    storage.pop_back();
    storage.pop_back();

    /* receive appendentries messages */
    r.send_appendentries(n->get_id());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
    ASSERT_TRUE(msg.isSome());
    MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

TEST(TestLeader, sends_appendentries_with_leader_commit)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);

    Index ci = r.committer().get_current_idx();
    for (std::size_t i = 0; i < 10; i++)
        storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    r.sync_log_and_nodes();

    r.accept_rep(NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, r.committer().get_current_idx()));
    EXPECT_EQ(ci+10, r.committer().get_current_idx());
    EXPECT_EQ(ci+10, r.committer().get_commit_idx());
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(raft::NodeId(3));
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
        EXPECT_EQ(ci+10, ae->leader_commit);
    }
}

TEST(TestLeader, sends_appendentries_with_prevLogIdx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    /*leader added noop log entry*/
    std::size_t count = storage.count();

    Exchanger sender(&r);
    sender.clear();

    bmcl::Option<const raft::Node&> n = r.nodes().get_node(raft::NodeId(2));

    /* receive appendentries messages witch contains noop*/
    r.send_appendentries(n->get_id());

    bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
    ASSERT_TRUE(msg.isSome());
    MsgAppendEntriesReq* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
    EXPECT_EQ(count, ae->data.prev_log_idx());


    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), true, count));
    EXPECT_EQ(count + 1, n->get_next_idx());

    /* add 1 entry */
    /* receive appendentries messages */
    storage.push_back(Entry(2, 100, raft::UserData("aaa", 4)));

    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        ASSERT_NE(nullptr, ae);

        EXPECT_EQ(count, ae->data.prev_log_idx());
        EXPECT_EQ(1, ae->data.count());
        EXPECT_EQ(100, ae->data.get_at_idx(ae->data.prev_log_idx() + 1)->id());
        EXPECT_EQ(2, ae->data.get_at_idx(ae->data.prev_log_idx() + 1)->term());
    }

    /* set next_idx */
    /* receive appendentries messages */
    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), true, count+1));
    EXPECT_EQ(count + 2, n->get_next_idx());

    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        ASSERT_NE(nullptr, ae);

        EXPECT_EQ(count + 1, ae->data.prev_log_idx());
    }
}

TEST(TestLeader, sends_appendentries_when_node_has_next_idx_of_0)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);
    sender.clear();

    bmcl::Option<const raft::Node&> n = r.nodes().get_node(raft::NodeId(2));

    /* receive appendentries messages */
    r.send_appendentries(n->get_id());
    MsgAppendEntriesReq*  ae;
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 0));
    EXPECT_EQ(1, n->get_next_idx());

    /* add an entry */
    /* receive appendentries messages */
    storage.push_back(Entry(1, 100, raft::UserData("aaa", 4)));
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(0, ae->data.prev_log_idx());
    }
}

/* 5.3 */
TEST(TestLeader, retries_appendentries_with_decremented_NextIdx_log_inconsistency)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);
    sender.clear();

    /* receive appendentries messages */
    r.send_appendentries(raft::NodeId(2));
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
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
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    Index count = storage.count();

    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(count + 1, storage.count());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    std::size_t start_index = storage.get_current_idx();

    /* the last applied idx will became start_index+1, and then start_index+2 */

    /* append entries - we need two */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    storage.push_back(Entry(1, 2, raft::UserData("aaa", 4)));
    storage.push_back(Entry(1, 3, raft::UserData("aaa", 4)));
    r.sync_log_and_nodes();

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, start_index+1));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, start_index+1));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(start_index+1, r.committer().get_commit_idx());
    r.tick();
    EXPECT_EQ(start_index+1, r.committer().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */

    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, start_index+2));
    EXPECT_EQ(start_index+1, r.committer().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, start_index+2));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(start_index+2, r.committer().get_commit_idx());
    r.tick();
    EXPECT_EQ(start_index+2, r.committer().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { raft::NodeId(1), raft::NodeId(2), raft::NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    r.add_node(1, raft::NodeId(4));
    EXPECT_FALSE(r.nodes().get_node(raft::NodeId(4))->is_voting());
//     r.add_node(2, raft::NodeId(5));
//     EXPECT_FALSE(r.nodes().get_node(raft::NodeId(5))->is_voting());

    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    //storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    r.add_entry(1, raft::UserData("aaa", 4));
    std::size_t current_index = storage.get_current_idx();

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, current_index));
    EXPECT_EQ(current_index, r.committer().get_commit_idx());
    /* leader will now have majority followers who have appended this log */
    r.tick();
    EXPECT_EQ(current_index, r.committer().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_duplicate_does_not_decrement_match_idx)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    storage.push_back(Entry(1, 2, raft::UserData("aaa", 4)));
    storage.push_back(Entry(1, 3, raft::UserData("aaa", 4)));

    /* receive msg 1 */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1));
    EXPECT_EQ(1, r.nodes().get_node(raft::NodeId(2))->get_match_idx());

    /* receive msg 2 */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 2));
    EXPECT_EQ(2, r.nodes().get_node(raft::NodeId(2))->get_match_idx());

    /* receive msg 1 - because of duplication ie. unreliable network */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1));
    EXPECT_EQ(2, r.nodes().get_node(raft::NodeId(2))->get_match_idx());
}

TEST(TestLeader, recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority)
{
    MemStorage storage;
    /* append entries - we need two */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    storage.push_back(Entry(1, 2, raft::UserData("aaa", 4)));
    storage.persist_term_vote(1, bmcl::None);

    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5) }, __Applier, &storage, &__Sender);

    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 1);

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 1));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 1));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.tick();
    EXPECT_EQ(0, r.committer().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(1, true, 2));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(1, true, 2));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.tick();
    EXPECT_EQ(0, r.committer().get_last_applied_idx());

    /* THIRD entry log application - noop entry automatically added by leader */
    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, storage.get_current_idx()));
    EXPECT_EQ(0, r.committer().get_commit_idx());
    r.accept_rep(raft::NodeId(3), MsgAppendEntriesRep(r.get_current_term(), true, storage.get_current_idx()));
    EXPECT_EQ(storage.get_current_idx(), r.committer().get_commit_idx());

    r.tick();
    EXPECT_EQ(storage.get_current_idx(), r.committer().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_jumps_to_lower_next_idx)
{
    MemStorage storage;
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    storage.push_back(Entry(2, 2, raft::UserData("aaa", 4)));
    storage.push_back(Entry(3, 3, raft::UserData("aaa", 4)));
    storage.push_back(Entry(4, 4, raft::UserData("aaa", 4)));
    /* noop entry with term=5 will be added by leader*/
    storage.persist_term_vote(4, bmcl::None);

    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    Exchanger sender(&r);

    bmcl::Option<const Node&> n = r.nodes().get_node(raft::NodeId(2));

    MsgAppendEntriesReq* ae;

    /* become leader sets next_idx to current_idx (leader added noop)*/
    prepare_leader(r);
    EXPECT_EQ(6, n->get_next_idx());

    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(5, ae->prev_log_term);
        EXPECT_EQ(5, ae->data.prev_log_idx());
    }

    /* receive mock success responses */
    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 1));
    EXPECT_EQ(2, n->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_term);
        EXPECT_EQ(1, ae->data.prev_log_idx());
    }

    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_decrements_to_lower_next_idx)
{
    MemStorage storage;
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));
    storage.push_back(Entry(2, 2, raft::UserData("aaa", 4)));
    storage.push_back(Entry(3, 3, raft::UserData("aaa", 4)));
    storage.push_back(Entry(4, 4, raft::UserData("aaa", 4)));
    /* noop entry with term=5 will be added by leader*/
    storage.persist_term_vote(4, bmcl::None);

    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    Exchanger sender(&r);
    bmcl::Option<const Node&> n = r.nodes().get_node(raft::NodeId(2));

    MsgAppendEntriesReq* ae;

    /* become leader sets next_idx to current_idx */
    prepare_leader(r);
    EXPECT_EQ(6, n->get_next_idx());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
    * server will be waiting for response */
    r.send_appendentries(n->get_id());
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(5, ae->prev_log_term);
        EXPECT_EQ(5, ae->data.prev_log_idx());
    }

    /* receive mock success responses */
    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 4));
    EXPECT_EQ(5, n->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->data.prev_log_idx());
    }

    /* receive mock success responses */
    r.accept_rep(n->get_id(), MsgAppendEntriesRep(r.get_current_term(), false, 4));
    EXPECT_EQ(4, n->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(3, ae->prev_log_term);
        EXPECT_EQ(3, ae->data.prev_log_idx());
    }

    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_retry_only_if_leader)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    Exchanger sender(&r);

    prepare_leader(r);
    sender.clear();
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    raft::TermId term = r.get_current_term();
    storage.push_back(Entry(term, 1, raft::UserData("aaa", 4)));

    r.send_appendentries(raft::NodeId(2));
    r.send_appendentries(raft::NodeId(3));

    EXPECT_TRUE(sender.poll_msg_data(r).isSome());
    EXPECT_TRUE(sender.poll_msg_data(r).isSome());

    prepare_follower(r);

    /* receive mock success responses */
    bmcl::Option<raft::Error> e = r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(term, true, 1));
    EXPECT_TRUE(e.isSome());
    EXPECT_EQ(raft::Error::NotLeader, e.unwrap());
    EXPECT_FALSE(sender.poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_from_unknown_node_fails)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    /* receive mock success responses */
    EXPECT_TRUE(r.accept_rep(raft::NodeId(4), MsgAppendEntriesRep(r.get_current_term(), true, 0)).isSome());
}

TEST(TestLeader, recv_entry_resets_election_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), true, __Applier, &storage, &__Sender);
    r.timer().set_timeout(Time(200), 5);
    prepare_leader(r);

    r.tick(Time(900));

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());
}

TEST(TestLeader, recv_entry_is_committed_returns_0_if_not_committed)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2)}, __Applier, &storage, &__Sender);
    prepare_leader(r);
    std::size_t count = storage.count();

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("aaa", 4));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(EntryState::NotCommitted, r.committer().entry_get_state(cr.unwrap()));
    EXPECT_EQ(0, r.committer().get_commit_idx());

    r.accept_rep(NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, r.committer().get_current_idx()));
    EXPECT_EQ(count + 1, r.committer().get_commit_idx()); //initial storage state + user's entry

    EXPECT_EQ(EntryState::Committed, r.committer().entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_is_committed_returns_neg_1_if_invalidated)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    Index ci = r.committer().get_current_idx();

    /* receive entry */
    auto cr = r.add_entry(9, raft::UserData("aaa", 4));
    ASSERT_TRUE(cr.isOk());
    EXPECT_EQ(EntryState::NotCommitted, r.committer().entry_get_state(cr.unwrap()));
    EXPECT_EQ(r.get_current_term(), cr.unwrap().term);
    EXPECT_EQ(ci + 1, cr.unwrap().idx);
    EXPECT_EQ(ci + 1, r.committer().get_current_idx());
    EXPECT_EQ(0, r.committer().get_commit_idx());

    /* append entry that invalidates entry message */

    Entry e(r.get_current_term() + 1, 999, raft::UserData("aaa", 4));
    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term() + 1, 0, ci + 1, 0, DataHandler(&e, ci, 1)));
    ASSERT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(ci + 1, r.committer().get_current_idx());
    EXPECT_EQ(ci + 1, r.committer().get_commit_idx());
    EXPECT_EQ(EntryState::Invalidated, r.committer().entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_does_not_send_new_appendentries_to_slow_nodes)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    Exchanger sender(&r);

    /* make the node slow */
    r.accept_rep(NodeId(2), MsgAppendEntriesRep(r.get_current_term(), false, 0));
    EXPECT_EQ(1, r.nodes().get_node(NodeId(2))->get_next_idx());
    sender.clear();

    /* append entries */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));

    /* receive entry */
    auto cr = r.add_entry(1, raft::UserData("bbb", 4));
    EXPECT_TRUE(cr.isOk());

    /* check if the slow node got sent this appendentries */
    {
        bmcl::Option<msg_t> msg = sender.poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }
}

TEST(TestLeader, recv_appendentries_response_failure_does_not_set_node_nextid_to_0)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    /* append entries */
    storage.push_back(Entry(1, 1, raft::UserData("aaa", 4)));

    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::NodeId(2));

    /* receive mock success response */
    bmcl::Option<const Node&> p = r.nodes().get_node(raft::NodeId(2));
    ASSERT_TRUE(p.isSome());
    r.accept_rep(p->get_id(), MsgAppendEntriesRep(1, false, 0));
    EXPECT_EQ(1, p->get_next_idx());
    r.accept_rep(p->get_id(), MsgAppendEntriesRep(1, false, 0));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_increment_idx_of_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    bmcl::Option<const raft::Node&> n = r.nodes().get_node(raft::NodeId(2));
    ASSERT_TRUE(n.isSome());
    Index nex_idx = n->get_next_idx();

    storage.push_back(Entry::user_empty(1, 1));
    storage.push_back(Entry::user_empty(1, 2));
    storage.push_back(Entry::user_empty(1, 3));
    storage.push_back(Entry::user_empty(1, 4));

    /* receive mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term(), true, storage.get_current_idx()));
    EXPECT_NE(nex_idx, n->get_next_idx());
    EXPECT_EQ(storage.get_current_idx() + 1, n->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_drop_message_if_term_is_old)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    prepare_follower(r);
    prepare_leader(r);
    EXPECT_GT(r.get_current_term(), 1);
    std::size_t ci = storage.get_current_idx();

    bmcl::Option<const Node&> n = r.nodes().get_node(raft::NodeId(2));
    ASSERT_TRUE(n.isSome());
    EXPECT_EQ(ci+1, n->get_next_idx());

    /* receive OLD mock success responses */
    r.accept_rep(raft::NodeId(2), MsgAppendEntriesRep(r.get_current_term() - 1, true, 99));
    EXPECT_EQ(ci+1, n->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_steps_down_if_term_is_newer)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);

    prepare_follower(r);
    prepare_leader(r);

    /* receive NEW mock failed responses */
    r.accept_rep(raft::NodeId(2), raft::MsgAppendEntriesRep(r.get_current_term() + 1, false, 3));
    EXPECT_TRUE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term() + 1, 5, 0, 0));
    EXPECT_TRUE(aer.isOk());

    /* after more recent appendentries from node 2, node 1 should
     * consider node 2 the leader. */
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(raft::NodeId(2), r.get_current_leader());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer_term)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2) }, __Applier, &storage, &__Sender);
    prepare_leader(r);

    auto aer = r.accept_req(raft::NodeId(2), MsgAppendEntriesReq(r.get_current_term() + 1, 5, 0, 0));
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(r.is_follower());
}

TEST(TestLeader, sends_empty_appendentries_every_request_timeout)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    Exchanger sender(&r);

    EXPECT_EQ(0, r.timer().get_timeout_elapsed().count());

    prepare_leader(r);
    sender.clear();

    bmcl::Option<msg_t> msg;
    MsgAppendEntriesReq* ae;

    r.tick(r.timer().get_request_timeout());
    /* receive appendentries messages for both nodes */
    {
        msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }

    /* force request timeout */
    r.tick(3*r.timer().get_request_timeout());
    {
        msg = sender.poll_msg_data(r);
        ASSERT_TRUE(msg.isSome());
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
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);

    prepare_candidate(r);

    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, raft::ReqVoteState::Granted));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    auto rvr = r.accept_req(raft::NodeId(3), MsgVoteReq(1, 0, 0, false));
    EXPECT_TRUE(rvr.isOk());
    EXPECT_EQ(raft::ReqVoteState::NotGranted, rvr.unwrap().vote_granted);
}

TEST(TestLeader, recv_requestvote_responds_with_granting_if_term_is_higher)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);

    prepare_candidate(r);

    r.accept_rep(raft::NodeId(2), MsgVoteRep(1, raft::ReqVoteState::Granted));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    r.accept_req(raft::NodeId(3), MsgVoteReq(2, 0, 0, false));
    EXPECT_TRUE(r.is_follower());
}

TEST(TestLeader, remove_other_node)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, &__Sender);
    prepare_leader(r);
    Index log_count = storage.count();

    TermId t = r.get_current_term();

    {
        auto e = r.remove_node(1, NodeId(2));
        EXPECT_TRUE(e.isOk());
        EXPECT_EQ(log_count + 1, storage.count());
        const Entry& last = storage.back().unwrap();
        EXPECT_TRUE(last.isInternal());
        EXPECT_EQ(InternalData::RemoveNode, last.getInternalData()->type);
    }

    {
        auto e = r.accept_rep(NodeId(2), MsgAppendEntriesRep(t, true, 1));
        EXPECT_TRUE(e.isSome());
        EXPECT_EQ(Error::NodeUnknown, e.unwrap());
    }

    {
        auto e = r.accept_rep(NodeId(3), MsgAppendEntriesRep(t, true, 1));
        EXPECT_FALSE(e.isSome());
    }

    r.tick();

    EXPECT_EQ(2, r.nodes().count());
    EXPECT_FALSE(r.nodes().get_node(NodeId(2)).isSome());
}

TEST(TestLeader, remove_me)
{
    MemStorage storage;
    raft::Server r(raft::NodeId(1), { NodeId(1), NodeId(2), NodeId(3) }, __Applier, &storage, nullptr);
    Exchanger sender(&r);
    prepare_leader(r);
    EXPECT_TRUE(r.get_current_leader().isSome());

    TermId t = r.get_current_term();

    {
        sender.clear();
        auto e = r.remove_node(1, r.nodes().get_my_id());
        EXPECT_FALSE(e.isErr());
        r.tick();
        EXPECT_FALSE(r.is_shutdown());
        EXPECT_EQ(2, r.nodes().count());
        EXPECT_FALSE(r.nodes().get_node(r.nodes().get_my_id()).isSome());

        bmcl::Option<msg_t> msg;

        msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());

        msg = sender.poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());

        msg = sender.poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }

    {
        auto e = r.accept_rep(NodeId(2), MsgAppendEntriesRep(t, true, storage.get_current_idx()));
        EXPECT_FALSE(e.isSome());
    }

    {
        auto e = r.accept_rep(NodeId(3), MsgAppendEntriesRep(t, true, storage.get_current_idx()));
        EXPECT_FALSE(e.isSome());
    }

    r.tick();
    EXPECT_TRUE(r.is_shutdown());
}
