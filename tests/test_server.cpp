#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"
// TODO: leader doesn't timeout and cause election

using namespace raft;

static bmcl::Option<raft::Error> __raft_persist_term(raft::Server* raft, std::size_t val)
{
    return bmcl::None;
}

static bmcl::Option<raft::Error> __raft_persist_vote(raft::Server* raft, std::size_t val)
{
    return bmcl::None;
}

int __raft_applylog(const raft::Server* raft, const raft_entry_t& ety, std::size_t idx)
{
    return 0;
}

bmcl::Option<raft::Error> __raft_send_requestvote(raft::Server* raft, const msg_requestvote_t& msg)
{
    return bmcl::None;
}

bmcl::Option<raft::Error> __raft_send_appendentries(raft::Server* raft, const raft::node_id& node, const msg_appendentries_t& msg)
{
    return bmcl::None;
}

raft_cbs_t generic_funcs()
{
    raft_cbs_t f = {0};
    f.persist_term = __raft_persist_term;
    f.persist_vote = __raft_persist_vote;
    f.send_appendentries = __raft_send_appendentries;
    f.send_requestvote = __raft_send_requestvote;
    f.applylog = __raft_applylog;
    return f;
};

TEST(TestServer, voted_for_records_who_we_voted_for)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.vote_for_nodeid(raft::node_id(1));
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
}

TEST(TestServer, get_my_node)
{
    raft::Server r(raft::node_id(1), true);
    r.nodes().add_node(raft::node_id(2));
    EXPECT_EQ(raft::node_id(1), r.nodes().get_my_id());
    EXPECT_NE(raft::node_id(2), r.nodes().get_my_id());
}

TEST(TestServer, idx_starts_at_1)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(0, r.log().get_current_idx());

    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_EQ(1, r.log().get_current_idx());
}

TEST(TestServer, currentterm_defaults_to_0)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, set_currentterm_sets_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.set_current_term(5);
    EXPECT_EQ(5, r.get_current_term());
}

TEST(TestServer, voting_results_in_voting)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(9));

    r.vote_for_nodeid(raft::node_id(2));
    EXPECT_EQ(raft::node_id(2), r.get_voted_for());
    r.vote_for_nodeid(raft::node_id(9));
    EXPECT_EQ(raft::node_id(9), r.get_voted_for());
}

TEST(TestServer, election_start_increments_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.set_current_term(1);
    r.election_start();
    EXPECT_EQ(2, r.get_current_term());
}

TEST(TestServer, set_state)
{
    raft::Server r(raft::node_id(1), true);
    r.set_state(raft_state_e::LEADER);
    EXPECT_EQ(raft_state_e::LEADER, r.get_state());
}

TEST(TestServer, starts_as_follower)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(raft_state_e::FOLLOWER, r.get_state());
}

TEST(TestServer, starts_with_election_timeout_of_1000ms)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(std::chrono::milliseconds(1000), r.get_election_timeout());
}

TEST(TestServer, starts_with_request_timeout_of_200ms)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(std::chrono::milliseconds(200), r.get_request_timeout());
}

TEST(TestServer, entry_append_increases_logidx)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(0, r.log().get_current_idx());
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_EQ(1, r.log().get_current_idx());
}

TEST(TestServer, append_entry_means_entry_gets_current_term)
{
    raft::Server r(raft::node_id(1), true);
    EXPECT_EQ(0, r.log().get_current_idx());
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_EQ(1, r.log().get_current_idx());
}

TEST(TestServer, append_entry_is_retrievable)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.set_state(raft_state_e::CANDIDATE);

    r.set_current_term(5);
    raft_entry_t ety(1, 100, raft::raft_entry_data_t("aaa", 4));
    r.entry_append(ety);

    bmcl::Option<const raft_entry_t&> kept =  r.log().get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_NE(nullptr, kept.unwrap().data.buf);
    EXPECT_EQ(ety.data.len, kept.unwrap().data.len);
    EXPECT_EQ(kept.unwrap().data.buf, ety.data.buf);
}

static int __raft_logentry_offer(const raft::Server* raft, const raft_entry_t& ety, std::size_t ety_idx)
{
    EXPECT_EQ(ety_idx, 1);
    //ety->data.buf = udata;
    return 0;
}

TEST(TestServer, append_entry_user_can_set_data_buf)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.set_state(raft_state_e::CANDIDATE);
    r.set_current_term(5);
    raft_entry_t ety(1, 100, raft::raft_entry_data_t("aaa", 4));
    r.entry_append(ety);
    bmcl::Option<const raft_entry_t&> kept =  r.log().get_at_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_NE(nullptr, kept.unwrap().data.buf);
    EXPECT_EQ(kept.unwrap().data.buf, ety.data.buf);
}

#if 0
/* TODO: no support for duplicate detection yet */
void
T_estRaft_server_append_entry_not_sucessful_if_entry_with_id_already_appended()
{
    void *r;
    raft_entry_t ety;
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    r = raft_new();
    EXPECT_EQ(1, r.raft_get_current_idx());
    r.raft_append_entry(ety);
    r.raft_append_entry(ety);
    EXPECT_EQ(2, r.raft_get_current_idx());

    /* different ID so we can be successful */
    ety.id = 2;
    r.raft_append_entry(ety);
    EXPECT_EQ(3, r.raft_get_current_idx());
}
#endif

TEST(TestServer, entry_is_retrieveable_using_idx)
{
    char *str = "aaa";
    char *str2 = "bbb";

    raft::Server r(raft::node_id(1), true);

    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t(str, 4)));

    /* different ID so we can be successful */
    r.entry_append(raft_entry_t(1, 2, raft::raft_entry_data_t(str2, 4)));

    bmcl::Option<const raft_entry_t&> ety_appended = r.log().get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str2, 3));
}


/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (§5.3) */
TEST(TestServer, increment_lastApplied_when_lastApplied_lt_commitidx)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());

    /* must be follower */
    r.set_state(raft_state_e::FOLLOWER);
    r.set_current_term(1);

    /* need at least one entry */
    r.log().entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.log().set_commit_idx(1);

    /* let time lapse */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_NE(0, r.log().get_last_applied_idx());
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestServer, apply_entry_increments_last_applied_idx)
{
    raft_cbs_t funcs = {0};
    funcs.applylog = __raft_applylog;

    raft::Server r(raft::node_id(1), true, funcs);

    r.log().entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.log().set_commit_idx(1);
    r.log().entry_apply_one();
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestServer, periodic_elapses_election_timeout)
{
    raft::Server r(raft::node_id(1), true);
    /* we don't want to set the timeout to zero */
    r.set_election_timeout(std::chrono::milliseconds(1000));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(0));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(100));
    EXPECT_EQ(100, r.get_timeout_elapsed().count());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.is_leader());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node)
{
    raft::Server r(raft::node_id(1), false);
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.is_leader());
    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_not_start_election_if_there_are_no_voting_nodes)
{
    raft::Server r(raft::node_id(1), false);
    r.nodes().add_non_voting_node(raft::node_id(2));
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_EQ(0, r.get_current_term());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_node)
{
    raft::Server r(raft::node_id(1), true);
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_non_voting_node(raft::node_id(2));
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.is_leader());
}

TEST(TestServer, recv_entry_auto_commits_if_we_are_the_only_node)
{
    raft::Server r(raft::node_id(1), true);
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.become_leader();
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* receive entry */
    auto cr = r.accept_entry(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());
    EXPECT_EQ(1, r.log().get_commit_idx());
}

TEST(TestServer, recv_entry_fails_if_there_is_already_a_voting_change)
{
    raft::Server r(raft::node_id(1), true);
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.become_leader();
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* entry message */
    msg_entry_t ety(0, 1, raft::raft_entry_data_t("aaa", 4));
    ety.type = logtype_e::ADD_NODE;

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
    EXPECT_FALSE(raft::Nodes::raft_votes_is_majority(3, 1));

    /* 2 of 3 = win */
    EXPECT_TRUE(raft::Nodes::raft_votes_is_majority(3, 2));

    /* 2 of 5 = lose */
    EXPECT_FALSE(raft::Nodes::raft_votes_is_majority(5, 2));

    /* 3 of 5 = win */
    EXPECT_TRUE(raft::Nodes::raft_votes_is_majority(5, 3));

    /* 2 of 1?? This is an error */
    EXPECT_FALSE(raft::Nodes::raft_votes_is_majority(1, 2));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_not_granted)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(1, raft_request_vote::NOT_GRANTED));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(3);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(2, raft_request_vote::GRANTED));
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_increase_votes_for_me)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
    EXPECT_EQ(1, r.get_current_term());

    r.become_candidate();
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    bmcl::Option<raft::Error> e = r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(2, raft_request_vote::GRANTED));
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

TEST(TestServer, recv_requestvote_response_must_be_candidate_to_receive)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    r.become_leader();

    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(1, raft_request_vote::GRANTED));
    EXPECT_EQ(0, r.nodes().get_nvotes_for_me(r.get_voted_for()));
}

/* Reply false if term < currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_false_if_term_less_than_current_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(2);

    /* term is less than current term */
    msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(1, 0, 0));
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.vote_granted);
}

TEST(TestServer, leader_recv_requestvote_does_not_step_down)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);
    r.vote_for_nodeid(raft::node_id(1));
    r.become_leader();
    EXPECT_TRUE(r.is_leader());

    /* term is less than current term */
    r.accept_requestvote(raft::node_id(2), msg_requestvote_t(1, 0, 0));
    EXPECT_EQ(raft::node_id(1), r.get_current_leader());
}

/* Reply true if term >= currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    /* term is less than current term */
    msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(2, 1, 0));
    EXPECT_EQ(raft_request_vote::GRANTED, rvr.vote_granted);
}

TEST(TestServer, recv_requestvote_reset_timeout)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_periodic(std::chrono::milliseconds(900));

    msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(2, 1, 0));
    EXPECT_EQ(raft_request_vote::GRANTED, rvr.vote_granted);
    EXPECT_EQ(0, r.get_timeout_elapsed().count());
}

TEST(TestServer, recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.become_candidate();
    r.set_current_term(1);
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());

    /* current term is less than term */
    r.accept_requestvote(raft::node_id(2), msg_requestvote_t(2, 1, 0));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::node_id(2), r.get_voted_for());
}

TEST(TestServer, recv_requestvote_depends_on_candidate_id)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.become_candidate();
    r.set_current_term(1);
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());

    /* current term is less than term */
    msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(2, 1, 0));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_EQ(raft::node_id(2), r.get_voted_for());
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (§5.2, §5.4) */
TEST(TestServer, recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(0));
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    /* vote for self */
    r.vote_for_nodeid(raft::node_id(1));

    {
        msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(1, 1, 1));
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.vote_granted);
    }

    /* vote for ID 0 */
    r.vote_for_nodeid(raft::node_id(0));
    {
        msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(1, 1, 1));
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.vote_granted);
    }
}

TEST(TestFollower, becomes_follower_is_follower)
{
    raft::Server r(raft::node_id(1), true);
    r.become_follower();
    EXPECT_TRUE(r.is_follower());
}

TEST(TestFollower, becomes_follower_does_not_clear_voted_for)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.vote_for_nodeid(raft::node_id(1));
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
    r.become_follower();
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
}

/* 5.1 */
TEST(TestFollower, recv_appendentries_reply_false_if_term_less_than_currentterm)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    /* no leader known at this point */
    EXPECT_FALSE(r.get_current_leader().isSome());

    /* term is low */
    msg_appendentries_t ae(1);

    /*  higher current term */
    r.set_current_term(5);
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_FALSE(aer.unwrap().success);
    /* rejected appendentries doesn't change the current leader. */
    EXPECT_FALSE(r.get_current_leader().isSome());
}

TEST(TestFollower, recv_appendentries_from_unknown_node_fails)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    auto aer = r.accept_appendentries(raft::node_id(3), msg_appendentries_t(1));
    EXPECT_FALSE(aer.isOk());
}

/* TODO: check if test case is needed */
TEST(TestFollower, recv_appendentries_updates_currentterm_if_term_gt_currentterm)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    /*  older currentterm */
    r.set_current_term(1);
    EXPECT_FALSE(r.get_current_leader().isSome());

    /*  newer term for appendentry */
    /* no prev log idx */
    msg_appendentries_t ae(2, 0);

    /*  appendentry has newer term, so we change our currentterm */
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, aer.unwrap().term);
    /* term has been updated */
    EXPECT_EQ(2, r.get_current_term());
    /* and leader has been updated */
    EXPECT_EQ(raft::node_id(2), r.get_current_leader());
}

TEST(TestFollower, recv_appendentries_does_not_log_if_no_entries_are_specified)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::FOLLOWER);

    /*  log size s */
    EXPECT_EQ(0, r.log().count());

    /* receive an appendentry with commit */
    msg_appendentries_t ae(1);
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    ae.n_entries = 0;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.log().count());
}

TEST(TestFollower, recv_appendentries_increases_log)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::FOLLOWER);

    /*  log size s */
    EXPECT_EQ(0, r.log().count());

    /* receive an appendentry with commit */
    msg_appendentries_t ae(3);
    ae.term = 3;
    ae.prev_log_term = 1;
    /* first appendentries msg */
    ae.prev_log_idx = 0;
    ae.leader_commit = 5;
    /* include one entry */
    msg_entry_t ety(2, 1, raft::raft_entry_data_t("aaa", 4));
    /* check that old terms are passed onto the log */
    ae.entries = &ety;
    ae.n_entries = 1;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().count());
    bmcl::Option<const raft_entry_t&> log = r.log().get_at_idx(1);
    EXPECT_TRUE(log.isSome());
    EXPECT_EQ(2, log.unwrap().term);
}

/*  5.3 */
TEST(TestFollower, recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    /* term is different from appendentries */
    r.set_current_term(2);
    // TODO at log manually?

    /* log idx that server doesn't have */
    msg_appendentries_t ae(2);
    ae.prev_log_idx = 1;
    /* prev_log_term is less than current term (ie. 2) */
    ae.prev_log_term = 1;
    /* include one entry */
    msg_entry_t ety(0, 1, raft::raft_entry_data_t("aaa", 4));
    ae.entries = &ety;
    ae.n_entries = 1;

    /* trigger reply */
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    /* reply is false */
    EXPECT_FALSE(aer.unwrap().success);
}

static void __create_mock_entries_for_conflict_tests(raft::Server* r, char** strs)
{
    bmcl::Option<const raft_entry_t&> ety_appended;

    /* increase log size */
    char *str1 = strs[0];
    r->entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t(str1, 3)));
    EXPECT_EQ(1, r->log().count());

    /* this log will be overwritten by a later appendentries */
    char *str2 = strs[1];
    r->entry_append(raft_entry_t(1, 2, raft::raft_entry_data_t(str2, 3)));
    EXPECT_EQ(2, r->log().count());
    ety_appended = r->log().get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str2, 3));

    /* this log will be overwritten by a later appendentries */
    char *str3 = strs[2];
    r->entry_append(raft_entry_t(1, 3, raft::raft_entry_data_t(str3, 4)));
    EXPECT_EQ(3, r->log().count());
    ety_appended = r->log().get_at_idx(3);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str3, 3));
}

/* 5.3 */
TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    __create_mock_entries_for_conflict_tests(&r, strs);

    /* pass a appendentry that is newer  */

    msg_appendentries_t ae(2);
    /* entries from 2 onwards will be overwritten by this appendentries message */
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include one entry */
    char *str4 = "444";
    msg_entry_t mety = msg_entry_t(0, 4, raft_entry_data_t(str4, 3));
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
    /* str1 is still there */

    bmcl::Option<const raft_entry_t&> ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, strs[0], 3));
    /* str4 has overwritten the last 2 entries */

    ety_appended = r.log().get_at_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str4, 3));
}

TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    __create_mock_entries_for_conflict_tests(&r, strs);

    /* pass a appendentry that is newer  */

    msg_appendentries_t ae(2);
    /* ALL append entries will be overwritten by this appendentries message */
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include one entry */
    char *str4 = "444";
    msg_entry_t mety = raft_entry_t(0, 4, raft_entry_data_t(str4, 3));
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().count());
    /* str1 is gone */
    bmcl::Option<const raft_entry_t&> ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str4, 3));
}

TEST(TestFollower, recv_appendentries_delete_entries_if_current_idx_greater_than_prev_log_idx)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    bmcl::Option<const raft_entry_t&> ety_appended;
    
    __create_mock_entries_for_conflict_tests(&r, strs);
    EXPECT_EQ(3, r.log().count());

    msg_appendentries_t ae(2);
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    msg_entry_t e(0, 1);
    ae.entries = &e;
    ae.n_entries = 1;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
    ety_appended = r.log().get_at_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, strs[0], 3));
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

TEST(TestFollower, recv_appendentries_add_new_entries_not_already_in_log)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[2] = {raft_entry_t(0, 1), raft_entry_t(0, 2) };
    ae.entries = e;
    ae.n_entries = 2;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
}

TEST(TestFollower, recv_appendentries_does_not_add_dupe_entries_already_in_log)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include 1 entry */
    msg_entry_t e[2] = { raft_entry_t(0, 1), raft_entry_t(0, 2) };
    ae.entries = e;
    ae.n_entries = 1;

    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* still successful even when no raft_append_entry() happened! */
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
        EXPECT_EQ(1, r.log().count());
    }

    /* lets get the server to append 2 now! */
    ae.n_entries = 2;
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.log().count());
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
TEST(TestFollower, recv_appendentries_set_commitidx_to_prevLogIdx)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4] = {raft_entry_t(1, 1), raft_entry_t(1, 2), raft_entry_t(1, 3), raft_entry_t(1, 4)};
    ae.entries = e;
    ae.n_entries = 4;

    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    ae = {0};
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 4 because commitIDX is lower */
    EXPECT_EQ(4, r.log().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_set_commitidx_to_LeaderCommit)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4] = { raft_entry_t(1, 1), raft_entry_t(1, 2), raft_entry_t(1, 3), raft_entry_t(1, 4) };
    ae.entries = e;
    ae.n_entries = 4;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 3;
    ae.leader_commit = 3;
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 3 because leaderCommit is lower */
    EXPECT_EQ(3, r.log().get_commit_idx());
}

TEST(TestFollower, recv_appendentries_failure_includes_current_idx)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_current_term(1);

    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));

    /* receive an appendentry with commit */
    /* lower term means failure */
    msg_appendentries_t ae(0);
    ae.prev_log_term = 0;
    ae.prev_log_idx = 0;
    ae.leader_commit = 0;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_FALSE(aer.unwrap().success);
        EXPECT_EQ(1, aer.unwrap().current_idx);
    }

    /* try again with a higher current_idx */
    r.entry_append(raft_entry_t(1, 2, raft::raft_entry_data_t("aaa", 4)));
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_FALSE(aer.unwrap().success);
    EXPECT_EQ(2, aer.unwrap().current_idx);
}

TEST(TestFollower, becomes_candidate_when_election_timeout_occurs)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    /*  1 second election timeout */
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /*  1.001 seconds have passed */
    r.raft_periodic(std::chrono::milliseconds(1001));

    /* is a candidate now */
    EXPECT_TRUE(r.is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, dont_grant_vote_if_candidate_has_a_less_complete_log)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */

    r.set_current_term(1);

    /* server's idx are more up-to-date */
    r.entry_append(raft_entry_t(1, 100, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(2, 101, raft::raft_entry_data_t("aaa", 4)));

    /* vote not granted */
    {
        msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(1, 1, 1));
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.vote_granted);
    }

    /* approve vote, because last_log_term is higher */
    r.set_current_term(2);
    {
        msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(2, 1, 3));
        EXPECT_EQ(raft_request_vote::GRANTED, rvr.vote_granted);
    }
}

TEST(TestFollower, recv_appendentries_heartbeat_does_not_overwrite_logs)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e1(1, 1);
    ae.entries = &e1;
    ae.n_entries = 1;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE
     * NOTE: the server has received a response from the last AE so
     * prev_log_idx has been incremented */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e2[4] = { msg_entry_t(1, 2), msg_entry_t(1, 3), msg_entry_t(1, 4), msg_entry_t(1, 5) };
    ae.entries = e2;
    ae.n_entries = 4;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* receive a heartbeat
     * NOTE: the leader hasn't received the response to the last AE so it can 
     * only assume prev_Log_idx is still 1 */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 1;
    /* receipt of appendentries changes commit idx */
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    EXPECT_EQ(5, r.log().get_current_idx());
}

TEST(TestFollower, recv_appendentries_does_not_deleted_commited_entries)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    msg_appendentries_t ae(1);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[7] = {raft_entry_t(1, 1), raft_entry_t(1, 2), raft_entry_t(1, 3), raft_entry_t(1, 4), raft_entry_t(1, 5), raft_entry_t(1, 6), raft_entry_t(1, 7) };
    ae.entries = &e[0];
    ae.n_entries = 1;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* Follow up AE. Node responded with success */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = &e[1];
    ae.n_entries = 4;
    ae.leader_commit = 4;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = &e[1];
    ae.n_entries = 5;
    ae.leader_commit = 4;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.log().get_current_idx());
    EXPECT_EQ(4, r.log().get_commit_idx());

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 3;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = &e[4];
    ae.n_entries = 3;
    ae.leader_commit = 4;
    {
        auto aer = r.accept_appendentries(raft::node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.log().get_current_idx());
}

TEST(TestCandidate, becomes_candidate_is_candidate)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.become_candidate();
    EXPECT_TRUE(r.is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_increments_current_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());

    EXPECT_EQ(0, r.get_current_term());
    r.become_candidate();
    EXPECT_EQ(1, r.get_current_term());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_votes_for_self)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());

    EXPECT_FALSE(r.get_voted_for().isSome());
    r.become_candidate();
    EXPECT_EQ(r.nodes().get_my_id(), r.get_voted_for());
    EXPECT_EQ(1, r.nodes().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_resets_election_timeout)
{
    raft::Server r(raft::node_id(1), false, generic_funcs());

    r.set_election_timeout(std::chrono::milliseconds(1000));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(900));
    EXPECT_EQ(900, r.get_timeout_elapsed().count());

    r.become_candidate();
    /* time is selected randomly */
    EXPECT_TRUE(r.get_timeout_elapsed().count() < 1000);
}

TEST(TestFollower, recv_appendentries_resets_election_timeout)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_election_timeout(std::chrono::milliseconds(1000));

    r.raft_periodic(std::chrono::milliseconds(900));

    auto aer = r.accept_appendentries(raft::node_id(1), msg_appendentries_t(1));
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.get_timeout_elapsed().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_requests_votes_from_other_servers)
{
    raft::Server r(raft::node_id(1), true);
    raft::Server r2(raft::node_id(2), true);
    raft::Server r3(raft::node_id(3), true);
    Sender sender(&r);
    sender.add(&r2);
    sender.add(&r3);

    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = [&sender](raft::Server* raft, const msg_requestvote_t& msg) { return sender.sender_requestvote(raft, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));

    /* set term so we can check it gets included in the outbound message */
    r.set_current_term(2);

    /* becoming candidate triggers vote requests */
    r.become_candidate();

    bmcl::Option<msg_t> msg;
    msg_requestvote_t* rv;
    /* 2 nodes = 2 vote requests */
    msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_NE(2, rv->term);
    EXPECT_EQ(3, rv->term);

    /*  TODO: there should be more items */
    msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(3, rv->term);
}

/* Candidate 5.2 */
TEST(TestCandidate, election_timeout_and_no_leader_results_in_new_election)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_election_timeout(std::chrono::milliseconds(1000));

    /* server wants to be leader, so becomes candidate */
    r.become_candidate();
    EXPECT_EQ(1, r.get_current_term());

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));
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
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.nodes().add_node(raft::node_id(4));
    r.nodes().add_node(raft::node_id(5));
    EXPECT_EQ(5, r.nodes().count());

    /* vote for self */
    r.become_candidate();
    EXPECT_EQ(1, r.get_current_term());
    EXPECT_EQ(1, r.nodes().get_nvotes_for_me(r.get_voted_for()));

    /* a vote for us */
    /* get one vote */
    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(1, raft_request_vote::GRANTED));
    EXPECT_EQ(2, r.nodes().get_nvotes_for_me(r.get_voted_for()));
    EXPECT_FALSE(r.is_leader());

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    r.accept_requestvote_response(raft::node_id(3), msg_requestvote_response_t(1, raft_request_vote::GRANTED));
    EXPECT_TRUE(r.is_leader());
}

/* Candidate 5.2 */
TEST(TestCandidate, will_not_respond_to_voterequest_if_it_has_already_voted)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.vote_for_nodeid(raft::node_id(1));

    msg_requestvote_response_t rvr = r.accept_requestvote(raft::node_id(2), msg_requestvote_t(0, 0, 0));

    /* we've vote already, so won't respond with a vote granted... */
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.vote_granted);
}

/* Candidate 5.2 */
TEST(TestCandidate, requestvote_includes_logidx)
{
    raft::Server r(raft::node_id(1), true);
    raft::Server r2(raft::node_id(2), true);
    raft::Server r3(raft::node_id(3), true);
    Sender sender(&r);
    sender.add(&r2);
    sender.add(&r3);

    raft_cbs_t funcs = {0};
    funcs.send_requestvote = [&sender](raft::Server* raft, const msg_requestvote_t& msg) {return sender.sender_requestvote(raft, msg); };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.set_state(raft_state_e::CANDIDATE);

    r.set_current_term(5);
    /* 3 entries */
    r.entry_append(raft_entry_t(1, 100, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(1, 101, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(3, 102, raft::raft_entry_data_t("aaa", 4)));
    r.become_candidate(); //becoming candidate means new election term? so +1

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    msg_requestvote_t* rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(3, rv->last_log_idx);
    EXPECT_EQ(6, rv->term);
    EXPECT_EQ(3, rv->last_log_term);
    EXPECT_EQ(raft::node_id(1), msg.unwrap().sender);
}

TEST(TestCandidate, recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_current_term(1);
    r.set_state(raft_state_e::CANDIDATE);
    r.vote_for_nodeid(raft::node_id(2));
    EXPECT_FALSE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
    EXPECT_EQ(1, r.get_current_term());

    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(2, raft_request_vote::NOT_GRANTED));
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(2, r.get_current_term());
    EXPECT_FALSE(r.get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_frm_leader_results_in_follower)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::CANDIDATE);
    //r.vote_for_nodeid(bmcl::None);
    EXPECT_FALSE(r.is_follower());
    EXPECT_FALSE(r.get_current_leader().isSome());
    EXPECT_EQ(0, r.get_current_term());

    /* receive recent appendentries */
    auto aer = r.accept_appendentries(raft::node_id(2), msg_appendentries_t(1));
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(r.is_follower());
    /* after accepting a leader, it's available as the last known leader */
    EXPECT_EQ(raft::node_id(2), r.get_current_leader());
    EXPECT_EQ(1, r.get_current_term());
    EXPECT_FALSE(r.get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_from_same_term_results_in_step_down)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_current_term(1);
    r.become_candidate();
    EXPECT_FALSE(r.is_follower());
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());

    msg_appendentries_t ae(2);
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    auto aer = r.accept_appendentries(raft::node_id(2), ae);
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
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
}

TEST(TestLeader, becomes_leader_is_leader)
{
    raft::Server r(raft::node_id(1), true);
    r.become_leader();
    EXPECT_TRUE(r.is_leader());
}

TEST(TestLeader, becomes_leader_does_not_clear_voted_for)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.vote_for_nodeid(raft::node_id(1));
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
    r.become_leader();
    EXPECT_EQ(raft::node_id(1), r.get_voted_for());
}

TEST(TestLeader, when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));

    /* candidate to leader */
    r.set_state(raft_state_e::CANDIDATE);
    r.become_leader();

    int i;
    for (i = 2; i <= 3; i++)
    {
        bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::node_id(i));
        EXPECT_TRUE(p.isSome());
        EXPECT_EQ(r.log().get_current_idx() + 1, p->get_next_idx());
    }
}

/* 5.2 */
TEST(TestLeader, when_it_becomes_a_leader_sends_empty_appendentries)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));

    /* candidate to leader */
    r.set_state(raft_state_e::CANDIDATE);
    r.become_leader();

    /* receive appendentries messages for both nodes */
    bmcl::Option<msg_t> msg;
    msg_appendentries_t* ae;

    msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);

    msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
TEST(TestLeader, responds_to_entry_msg_when_entry_is_committed)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    /* I am the leader */
    r.set_state(raft_state_e::LEADER);
    EXPECT_EQ(0, r.log().count());

    /* receive entry */
    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());

    /* trigger response through commit */
    r.log().entry_apply_one();
}

void TestRaft_non_leader_recv_entry_msg_fails()
{
    raft::Server r(raft::node_id(1), true);
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::FOLLOWER);

    /* entry message */
    msg_entry_t ety(0, 1, raft::raft_entry_data_t("aaa", 4));

    /* receive entry */
    auto cr = r.accept_entry(ety);
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(raft::Error::NotLeader, cr.unwrapErr());
}

/* 5.3 */
TEST(TestLeader, sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));

    /* i'm leader */
    r.set_state(raft_state_e::LEADER);

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(p.isSome());
    p->set_next_idx(4);

    /* receive appendentries messages */
    r.send_appendentries(p.unwrap());

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

TEST(TestLeader, sends_appendentries_with_leader_commit)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };

    r.set_callbacks(funcs);
    r.nodes().add_node(raft::node_id(2));

    /* i'm leader */
    r.set_state(raft_state_e::LEADER);

    for (int i=0; i<10; i++)
    {
        r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    }

    r.log().set_commit_idx(10);

    /* receive appendentries messages */
    r.send_appendentries(raft::node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
        EXPECT_EQ(10, ae->leader_commit);
    }
}

TEST(TestLeader, sends_appendentries_with_prevLogIdx)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };

    r.set_callbacks(funcs);
    r.nodes().add_node(raft::node_id(2));

    /* i'm leader */
    r.set_state(raft_state_e::LEADER);

    /* receive appendentries messages */
    r.send_appendentries(raft::node_id(2));

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
    EXPECT_TRUE(msg.isSome());
    msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
    EXPECT_EQ(0, ae->prev_log_idx);

    bmcl::Option<raft::Node&> n = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(n.isSome());

    /* add 1 entry */
    /* receive appendentries messages */
    r.entry_append(msg_entry_t(2, 100, raft::raft_entry_data_t("aaa", 4)));
    n->set_next_idx(1);
    r.send_appendentries(n.unwrap());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
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
    r.send_appendentries(raft::node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_idx);
    }
}

TEST(TestLeader, sends_appendentries_when_node_has_next_idx_of_0)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };

    r.set_callbacks(funcs);
    r.nodes().add_node(raft::node_id(2));

    /* i'm leader */
    r.set_state(raft_state_e::LEADER);

    /* receive appendentries messages */
    r.send_appendentries(raft::node_id(2));
    msg_appendentries_t*  ae;
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* add an entry */
    /* receive appendentries messages */
    bmcl::Option<raft::Node&> n = r.nodes().get_node(raft::node_id(2));
    n->set_next_idx(1);
    r.entry_append(msg_entry_t(1, 100, raft::raft_entry_data_t("aaa", 4)));
    r.send_appendentries(n.unwrap());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(0, ae->prev_log_idx);
    }
}

/* 5.3 */
TEST(TestLeader, retries_appendentries_with_decremented_NextIdx_log_inconsistency)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));

    /* i'm leader */
    r.set_state(raft_state_e::LEADER);

    /* receive appendentries messages */
    r.send_appendentries(raft::node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
}

/*
 * If there exists an N such that N > commitidx, a majority
 * of matchidx[i] = N, and log[N].term == currentTerm:
 * set commitidx = N (§5.2, §5.4).  */
TEST(TestLeader, append_entry_to_log_increases_idxno)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.set_state(raft_state_e::LEADER);
    EXPECT_EQ(0, r.log().count());

    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.log().count());
}

#if 0
// TODO no support for duplicates
void T_estRaft_leader_doesnt_append_entry_if_unique_id_is_duplicate()
{
    void *r;

    /* 2 nodes */
    raft_node_configuration_t cfg[] = {
        { (void*)1 },
        { (void*)2 },
        { NULL     }
    };

    msg_entry_t ety;
    ety.id = 1;
    ety.data = raft::raft_entry_data_t("aaa", 4);

    r = raft_new();
    raft_set_configuration(r, cfg, 0);

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    EXPECT_EQ(0, r.raft_get_log_count());

    r.raft_recv_entry(1, &ety);
    EXPECT_EQ(1, r.raft_get_log_count());

    r.raft_recv_entry(1, &ety);
    EXPECT_EQ(1, r.raft_get_log_count());
}
#endif

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.nodes().add_node(raft::node_id(4));
    r.nodes().add_node(raft::node_id(5));
    r.set_callbacks(funcs);

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(1, 2, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(1, 3, raft::raft_entry_data_t("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_appendentries_response(raft::node_id(3), msg_appendentries_response_t(1, true, 1, 1));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(1, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */

    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 2, 2));
    EXPECT_EQ(1, r.log().get_commit_idx());
    r.accept_appendentries_response(raft::node_id(3), msg_appendentries_response_t(1, true, 2, 2));
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(2, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.log().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.nodes().add_non_voting_node(raft::node_id(4));
    r.nodes().add_non_voting_node(raft::node_id(5));

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(1, r.log().get_commit_idx());
    /* leader will now have majority followers who have appended this log */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.log().get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_duplicate_does_not_decrement_match_idx)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.set_callbacks(funcs);

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(msg_entry_t(1, 2, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(msg_entry_t(1, 3, raft::raft_entry_data_t("aaa", 4)));

    /* receive msg 1 */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(1, r.nodes().get_node(raft::node_id(2))->get_match_idx());

    /* receive msg 2 */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 2, 2));
    EXPECT_EQ(2, r.nodes().get_node(raft::node_id(2))->get_match_idx());

    /* receive msg 1 - because of duplication ie. unreliable network */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(2, r.nodes().get_node(raft::node_id(2))->get_match_idx());
}

TEST(TestLeader, recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.nodes().add_node(raft::node_id(4));
    r.nodes().add_node(raft::node_id(5));
    r.set_callbacks(funcs);

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(2);
    r.log().set_commit_idx(0);

    /* append entries - we need two */
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(1, 2, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(2, 3, raft::raft_entry_data_t("aaa", 4)));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));
    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_appendentries_response(raft::node_id(3), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.log().get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));
    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 2, 2));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_appendentries_response(raft::node_id(3), msg_appendentries_response_t(1, true, 2, 2));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.log().get_last_applied_idx());

    /* THIRD entry log application */
    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(2, true, 3, 3));
    EXPECT_EQ(0, r.log().get_commit_idx());
    r.accept_appendentries_response(raft::node_id(3), msg_appendentries_response_t(2, true, 3, 3));
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
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.set_callbacks(funcs);

    r.set_current_term(2);
    r.log().set_commit_idx(0);

    /* append entries */
    r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(msg_entry_t(2, 2, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(msg_entry_t(3, 3, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(msg_entry_t(4, 4, raft::raft_entry_data_t("aaa", 4)));

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    r.become_leader();
    bmcl::Option<raft::Node&> node = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(node.isSome());
    EXPECT_EQ(5, node->get_next_idx());

    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(2, false, 1, 0));
    EXPECT_EQ(2, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_term);
        EXPECT_EQ(1, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.sender_poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_decrements_to_lower_next_idx)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.set_callbacks(funcs);

    r.set_current_term(2);
    r.log().set_commit_idx(0);

    /* append entries */
    r.entry_append(raft_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(2, 2, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(3, 3, raft::raft_entry_data_t("aaa", 4)));
    r.entry_append(raft_entry_t(4, 4, raft::raft_entry_data_t("aaa", 4)));

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    r.become_leader();
    bmcl::Option<raft::Node&> node = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(node.isSome());
    EXPECT_EQ(5, node->get_next_idx());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(2, false, 4, 0));
    EXPECT_EQ(4, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(3, ae->prev_log_term);
        EXPECT_EQ(3, ae->prev_log_idx);
    }

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(2, false, 4, 0));
    EXPECT_EQ(3, node->get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(2, ae->prev_log_term);
        EXPECT_EQ(2, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.sender_poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_retry_only_if_leader)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.set_callbacks(funcs);

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */

    /* append entries - we need two */
    r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));

    r.send_appendentries(raft::node_id(2));
    r.send_appendentries(raft::node_id(3));

    EXPECT_TRUE(sender.sender_poll_msg_data(r).isSome());
    EXPECT_TRUE(sender.sender_poll_msg_data(r).isSome());

    r.become_follower();

    /* receive mock success responses */
    EXPECT_TRUE(r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1)).isSome());
    EXPECT_FALSE(sender.sender_poll_msg_data(r).isSome());
}

TEST(TestLeader, recv_appendentries_response_from_unknown_node_fails)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);

    /* receive mock success responses */
    EXPECT_TRUE(r.accept_appendentries_response(raft::node_id(4), msg_appendentries_response_t(1, true, 0, 0)).isSome());
}

TEST(TestLeader, recv_entry_resets_election_timeout)
{
    raft::Server r(raft::node_id(1), true);
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.set_state(raft_state_e::LEADER);

    r.raft_periodic(std::chrono::milliseconds(900));

    /* receive entry */
    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.get_timeout_elapsed().count());
}

TEST(TestLeader, recv_entry_is_committed_returns_0_if_not_committed)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);

    /* receive entry */
    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(raft_entry_state_e::NOTCOMMITTED, r.entry_get_state(cr.unwrap()));

    r.log().set_commit_idx(1);
    EXPECT_EQ(raft_entry_state_e::COMMITTED, r.entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_is_committed_returns_neg_1_if_invalidated)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);

    /* receive entry */
    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("aaa", 4)));
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(raft_entry_state_e::NOTCOMMITTED, r.entry_get_state(cr.unwrap()));
    EXPECT_EQ(1, cr.unwrap().term);
    EXPECT_EQ(1, cr.unwrap().idx);
    EXPECT_EQ(1, r.log().get_current_idx());
    EXPECT_EQ(0, r.log().get_commit_idx());

    /* append entry that invalidates entry message */
    msg_appendentries_t ae = {0};
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    msg_entry_t e(2, 999, raft::raft_entry_data_t("aaa", 4));
    ae.entries = &e;
    ae.n_entries = 1;
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.log().get_current_idx());
    EXPECT_EQ(1, r.log().get_commit_idx());
    EXPECT_EQ(raft_entry_state_e::INVALIDATED, r.entry_get_state(cr.unwrap()));
}

TEST(TestLeader, recv_entry_does_not_send_new_appendentries_to_slow_nodes)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);

    /* make the node slow */
    r.nodes().get_node(raft::node_id(2))->set_next_idx(1);

    /* append entries */
    r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));

    /* receive entry */
    auto cr = r.accept_entry(msg_entry_t(0, 1, raft::raft_entry_data_t("bbb", 4)));
    EXPECT_TRUE(cr.isOk());

    /* check if the slow node got sent this appendentries */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }
}

TEST(TestLeader, recv_appendentries_response_failure_does_not_set_node_nextid_to_0)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.set_callbacks(funcs);

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);
    r.log().set_commit_idx(0);

    /* append entries */
    r.entry_append(msg_entry_t(1, 1, raft::raft_entry_data_t("aaa", 4)));

    /* send appendentries -
     * server will be waiting for response */
    r.send_appendentries(raft::node_id(2));

    /* receive mock success response */
    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(p.isSome());
    r.accept_appendentries_response(p->get_id(), msg_appendentries_response_t(1, false, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
    r.accept_appendentries_response(p->get_id(), msg_appendentries_response_t(1, false, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_increment_idx_of_node)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(1);

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->get_next_idx());

    /* receive mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 0, 0));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_drop_message_if_term_is_old)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));

    /* I'm the leader */
    r.set_state(raft_state_e::LEADER);
    r.set_current_term(2);

    bmcl::Option<raft::Node&> p = r.nodes().get_node(raft::node_id(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->get_next_idx());

    /* receive OLD mock success responses */
    r.accept_appendentries_response(raft::node_id(2), msg_appendentries_response_t(1, true, 1, 1));
    EXPECT_EQ(1, p->get_next_idx());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(5);
    /* check that node 0 considers itself the leader */
    EXPECT_TRUE(r.is_leader());
    EXPECT_EQ(raft::node_id(1), r.get_current_leader());

    msg_appendentries_t ae(6);
    ae.prev_log_idx = 6;
    ae.prev_log_term = 5;
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    /* after more recent appendentries from node 1, node 0 should
     * consider node 1 the leader. */
    EXPECT_TRUE(r.is_follower());
    EXPECT_EQ(raft::node_id(1), r.get_current_leader());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer_term)
{
    raft::Server r(raft::node_id(1), true, generic_funcs());
    r.nodes().add_node(raft::node_id(2));

    r.set_state(raft_state_e::LEADER);
    r.set_current_term(5);

    msg_appendentries_t ae(6);
    ae.prev_log_idx = 5;
    ae.prev_log_term = 5;
    auto aer = r.accept_appendentries(raft::node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(r.is_follower());
}

TEST(TestLeader, sends_empty_appendentries_every_request_timeout)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    /* candidate to leader */
    r.set_state(raft_state_e::CANDIDATE);
    r.become_leader();

    bmcl::Option<msg_t> msg;
    msg_appendentries_t* ae;

    /* receive appendentries messages for both nodes */
    {
        msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.sender_poll_msg_data(r);
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.sender_poll_msg_data(r);
        EXPECT_FALSE(msg.isSome());
    }

    /* force request timeout */
    r.raft_periodic(std::chrono::milliseconds(501));
    {
        msg = sender.sender_poll_msg_data(r);
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
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_vote = __raft_persist_vote;
    funcs.persist_term = __raft_persist_term;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    r.election_start();

    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(1, raft_request_vote::GRANTED));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    msg_requestvote_response_t opt = r.accept_requestvote(raft::node_id(3), msg_requestvote_t(1, 0, 0));
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, opt.vote_granted);
}

TEST(TestLeader, recv_requestvote_responds_with_granting_if_term_is_higher)
{
    raft::Server r(raft::node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_vote = __raft_persist_vote;
    funcs.persist_term = __raft_persist_term;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = [&sender](raft::Server * raft, const raft::node_id & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
    r.set_callbacks(funcs);

    r.nodes().add_node(raft::node_id(2));
    r.nodes().add_node(raft::node_id(3));
    r.set_election_timeout(std::chrono::milliseconds(1000));
    r.set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.get_timeout_elapsed().count());

    r.election_start();

    r.accept_requestvote_response(raft::node_id(2), msg_requestvote_response_t(1, raft_request_vote::GRANTED));
    EXPECT_TRUE(r.is_leader());

    /* receive request vote from node 3 */
    r.accept_requestvote(raft::node_id(3), msg_requestvote_t(2, 0, 0));
    EXPECT_TRUE(r.is_follower());
}
