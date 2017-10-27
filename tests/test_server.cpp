#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"
// TODO: leader doesn't timeout and cause election


static bmcl::Option<RaftError> __raft_persist_term(Raft* raft, const int val)
{
    return bmcl::None;
}

static bmcl::Option<RaftError> __raft_persist_vote(Raft* raft, const int val)
{
    return bmcl::None;
}

int __raft_applylog(const Raft* raft, const raft_entry_t& ety, int idx)
{
    return 0;
}

bmcl::Option<RaftError> __raft_send_requestvote(Raft* raft, const RaftNode& node, const msg_requestvote_t& msg)
{
    return bmcl::None;
}

bmcl::Option<RaftError> __raft_send_appendentries(Raft* raft, const RaftNode& node, const msg_appendentries_t& msg)
{
    return bmcl::None;
}

raft_cbs_t generic_funcs()
{
    raft_cbs_t f = {0};
    f.persist_term = __raft_persist_term;
    f.persist_vote = __raft_persist_vote;
    return f;
};

TEST(TestServer, voted_for_records_who_we_voted_for)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_callbacks(generic_funcs());
    r.raft_vote_for_nodeid(raft_node_id(1));
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
}

TEST(TestServer, get_my_node)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(2));
    EXPECT_EQ(raft_node_id(1), r.raft_get_my_nodeid());
    EXPECT_NE(raft_node_id(2), r.raft_get_my_nodeid());
}

TEST(TestServer, idx_starts_at_1)
{
    Raft r(raft_node_id(1), true);
    EXPECT_EQ(0, r.raft_get_current_idx());

    raft_entry_t ety = {0};
    ety.data.buf = "aaa";
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    r.raft_append_entry(ety);
    EXPECT_EQ(1, r.raft_get_current_idx());
}

TEST(TestServer, currentterm_defaults_to_0)
{
    Raft r(raft_node_id(1), true);
    EXPECT_EQ(0, r.raft_get_current_term());
}

TEST(TestServer, set_currentterm_sets_term)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_callbacks(generic_funcs());
    r.raft_set_current_term(5);
    EXPECT_EQ(5, r.raft_get_current_term());
}

TEST(TestServer, voting_results_in_voting)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_callbacks(generic_funcs());
    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(9));

    r.raft_vote_for_nodeid(raft_node_id(2));
    EXPECT_EQ(raft_node_id(2), r.raft_get_voted_for());
    r.raft_vote_for_nodeid(raft_node_id(9));
    EXPECT_EQ(raft_node_id(9), r.raft_get_voted_for());
}

TEST(TestServer, add_node_makes_non_voting_node_voting)
{
    Raft r(raft_node_id(9), false);
    bmcl::Option<RaftNode&> n1 = r.raft_get_node(raft_node_id(9));

    EXPECT_TRUE(n1.isSome());
    EXPECT_FALSE(n1->raft_node_is_voting());
    r.raft_add_node(raft_node_id(9));
    EXPECT_TRUE(n1->raft_node_is_voting());
    EXPECT_EQ(1, r.raft_get_num_nodes());
}

TEST(TestServer, add_node_with_already_existing_id_is_not_allowed)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(9));
    r.raft_add_node(raft_node_id(11));

    EXPECT_FALSE(r.raft_add_node(raft_node_id(9)).isSome());
    EXPECT_FALSE(r.raft_add_node(raft_node_id(11)).isSome());
}

TEST(TestServer, add_non_voting_node_with_already_existing_id_is_not_allowed)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_non_voting_node(raft_node_id(9));
    r.raft_add_non_voting_node(raft_node_id(11));

    EXPECT_FALSE(r.raft_add_non_voting_node(raft_node_id(9)).isSome());
    EXPECT_FALSE(r.raft_add_non_voting_node(raft_node_id(11)).isSome());
}

TEST(TestServer, add_non_voting_node_with_already_existing_voting_id_is_not_allowed)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(9));
    r.raft_add_node(raft_node_id(11));

    EXPECT_FALSE(r.raft_add_non_voting_node(raft_node_id(9)).isSome());
    EXPECT_FALSE(r.raft_add_non_voting_node(raft_node_id(11)).isSome());
}

TEST(TestServer, remove_node)
{
    Raft r(raft_node_id(1), true);
    bmcl::Option<RaftNode&> n1 = r.raft_add_node(raft_node_id(2));
    bmcl::Option<RaftNode&> n2 = r.raft_add_node(raft_node_id(9));

    r.raft_remove_node(raft_node_id(2));
    EXPECT_FALSE(r.raft_get_node(raft_node_id(2)).isSome());
    EXPECT_TRUE(r.raft_get_node(raft_node_id(9)).isSome());
    r.raft_remove_node(raft_node_id(9));
    EXPECT_FALSE(r.raft_get_node(raft_node_id(9)).isSome());
}

TEST(TestServer, election_start_increments_term)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_callbacks(generic_funcs());
    r.raft_set_current_term(1);
    r.raft_election_start();
    EXPECT_EQ(2, r.raft_get_current_term());
}

TEST(TestServer, set_state)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    EXPECT_EQ(raft_state_e::RAFT_STATE_LEADER, r.raft_get_state());
}

TEST(TestServer, starts_as_follower)
{
    Raft r(raft_node_id(1), true);
    EXPECT_EQ(raft_state_e::RAFT_STATE_FOLLOWER, r.raft_get_state());
}

TEST(TestServer, starts_with_election_timeout_of_1000ms)
{
    Raft r(raft_node_id(1), true);
    EXPECT_EQ(std::chrono::milliseconds(1000), r.raft_get_election_timeout());
}

TEST(TestServer, starts_with_request_timeout_of_200ms)
{
    Raft r(raft_node_id(1), true);
    EXPECT_EQ(std::chrono::milliseconds(200), r.raft_get_request_timeout());
}

TEST(TestServer, entry_append_increases_logidx)
{
    raft_entry_t ety = {0};
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    Raft r(raft_node_id(1), true);
    EXPECT_EQ(0, r.raft_get_current_idx());
    r.raft_append_entry(ety);
    EXPECT_EQ(1, r.raft_get_current_idx());
}

TEST(TestServer, append_entry_means_entry_gets_current_term)
{
    raft_entry_t ety = {0};
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    Raft r(raft_node_id(1), true);
    EXPECT_EQ(0, r.raft_get_current_idx());
    r.raft_append_entry(ety);
    EXPECT_EQ(1, r.raft_get_current_idx());
}

TEST(TestServer, append_entry_is_retrievable)
{
    Raft r(raft_node_id(1), true, generic_funcs());
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);

    r.raft_set_current_term(5);
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    r.raft_append_entry(ety);

    bmcl::Option<const raft_entry_t&> kept =  r.raft_get_entry_from_idx(1);
    EXPECT_TRUE(kept.isSome());
    EXPECT_NE(nullptr, kept.unwrap().data.buf);
    EXPECT_EQ(ety.data.len, kept.unwrap().data.len);
    EXPECT_EQ(kept.unwrap().data.buf, ety.data.buf);
}

static int __raft_logentry_offer(const Raft* raft, const raft_entry_t& ety, int ety_idx)
{
    EXPECT_EQ(ety_idx, 1);
    //ety->data.buf = udata;
    return 0;
}

TEST(TestServer, append_entry_user_can_set_data_buf)
{
    raft_cbs_t funcs = { 0 };
    funcs.log_offer = __raft_logentry_offer;
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_set_current_term(5);
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    r.raft_append_entry(ety);
    bmcl::Option<const raft_entry_t&> kept =  r.raft_get_entry_from_idx(1);
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

    Raft r(raft_node_id(1), true);

    raft_entry_t e1 = {0};
    e1.term = 1;
    e1.id = 1;
    e1.data.buf = str;
    e1.data.len = 3;
    r.raft_append_entry(e1);

    /* different ID so we can be successful */
    raft_entry_t e2 = {0};
    e2.term = 1;
    e2.id = 2;
    e2.data.buf = str2;
    e2.data.len = 3;
    r.raft_append_entry(e2);

    bmcl::Option<const raft_entry_t&> ety_appended = r.raft_get_entry_from_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str2, 3));
}

TEST(TestServer, wont_apply_entry_if_we_dont_have_entry_to_apply)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_commit_idx(0);
    r.raft_set_last_applied_idx(0);

    r.raft_apply_entry();
    EXPECT_EQ(0, r.raft_get_last_applied_idx());
    EXPECT_EQ(0, r.raft_get_commit_idx());
}

TEST(TestServer, wont_apply_entry_if_there_isnt_a_majority)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_commit_idx(0);
    r.raft_set_last_applied_idx(0);

    r.raft_apply_entry();
    EXPECT_EQ(0, r.raft_get_last_applied_idx());
    EXPECT_EQ(0, r.raft_get_commit_idx());

    char *str = "aaa";
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = str;
    ety.data.len = 3;
    r.raft_append_entry(ety);
    r.raft_apply_entry();
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    EXPECT_EQ(0, r.raft_get_last_applied_idx());
    EXPECT_EQ(0, r.raft_get_commit_idx());
}

/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (§5.3) */
TEST(TestServer, increment_lastApplied_when_lastApplied_lt_commitidx)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.applylog = __raft_applylog;

    Raft r(raft_node_id(1), true, funcs);

    /* must be follower */
    r.raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);
    r.raft_set_current_term(1);
    r.raft_set_last_applied_idx(0);

    /* need at least one entry */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaa";
    ety.data.len = 3;
    r.raft_append_entry(ety);

    r.raft_set_commit_idx(1);

    /* let time lapse */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_NE(0, r.raft_get_last_applied_idx());
    EXPECT_EQ(1, r.raft_get_last_applied_idx());
}

TEST(TestServer, apply_entry_increments_last_applied_idx)
{
    raft_cbs_t funcs = {0};
    funcs.applylog = __raft_applylog;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_set_last_applied_idx(0);

    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaa";
    ety.data.len = 3;
    r.raft_append_entry(ety);
    r.raft_set_commit_idx(1);
    r.raft_apply_entry();
    EXPECT_EQ(1, r.raft_get_last_applied_idx());
}

TEST(TestServer, periodic_elapses_election_timeout)
{
    Raft r(raft_node_id(1), true);
    /* we don't want to set the timeout to zero */
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(0));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(100));
    EXPECT_EQ(100, r.raft_get_timeout_elapsed().count());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.raft_is_leader());
}

TEST(TestServer, election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node)
{
    Raft r(raft_node_id(1), false);
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_FALSE(r.raft_is_leader());
    EXPECT_EQ(0, r.raft_get_current_term());
}

TEST(TestServer, election_timeout_does_not_start_election_if_there_are_no_voting_nodes)
{
    Raft r(raft_node_id(1), false);
    r.raft_add_non_voting_node(raft_node_id(2));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_EQ(0, r.raft_get_current_term());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_node)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.raft_is_leader());
}

TEST(TestServer, election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node)
{
    raft_cbs_t funcs = {0};
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_non_voting_node(raft_node_id(2));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));

    EXPECT_TRUE(r.raft_is_leader());
}

TEST(TestServer, recv_entry_auto_commits_if_we_are_the_only_node)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_become_leader();
    EXPECT_EQ(0, r.raft_get_commit_idx());

    /* entry message */
    msg_entry_t ety = {0};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(ety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.raft_get_log_count());
    EXPECT_EQ(1, r.raft_get_commit_idx());
}

TEST(TestServer, recv_entry_fails_if_there_is_already_a_voting_change)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_become_leader();
    EXPECT_EQ(0, r.raft_get_commit_idx());

    /* entry message */
    msg_entry_t ety = {0};
    ety.type = raft_logtype_e::RAFT_LOGTYPE_ADD_NODE;
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    EXPECT_TRUE(r.raft_recv_entry(ety).isOk());
    EXPECT_EQ(1, r.raft_get_log_count());

    ety.id = 2;
    auto cr = r.raft_recv_entry(ety);
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(RaftError::OneVotiongChangeOnly, cr.unwrapErr());
    EXPECT_EQ(1, r.raft_get_commit_idx());
}

TEST(TestServer, cfg_sets_num_nodes)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(2));

    EXPECT_EQ(2, r.raft_get_num_nodes());
}

TEST(TestServer, cant_get_node_we_dont_have)
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(2));

    EXPECT_FALSE(r.raft_get_node(raft_node_id(0)).isSome());
    EXPECT_TRUE(r.raft_get_node(raft_node_id(1)).isSome());
    EXPECT_TRUE(r.raft_get_node(raft_node_id(2)).isSome());
    EXPECT_FALSE(r.raft_get_node(raft_node_id(3)).isSome());
}

/* If term > currentTerm, set currentTerm to term (step down if candidate or
 * leader) */
TEST(TestServer, votes_are_majority_is_true)
{
    /* 1 of 3 = lose */
    EXPECT_FALSE(Raft::raft_votes_is_majority(3, 1));

    /* 2 of 3 = win */
    EXPECT_TRUE(Raft::raft_votes_is_majority(3, 2));

    /* 2 of 5 = lose */
    EXPECT_FALSE(Raft::raft_votes_is_majority(5, 2));

    /* 3 of 5 = win */
    EXPECT_TRUE(Raft::raft_votes_is_majority(5, 3));

    /* 2 of 1?? This is an error */
    EXPECT_FALSE(Raft::raft_votes_is_majority(1, 2));
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_not_granted)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());

    msg_requestvote_response_t rvr = {0};
    rvr.term = 1;
    rvr.vote_granted = raft_request_vote::NOT_GRANTED;
    bmcl::Option<RaftError> e = r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());
}

TEST(TestServer, recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(3);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());

    msg_requestvote_response_t rvr = { 0 };
    rvr.term = 2;
    rvr.vote_granted = raft_request_vote::GRANTED;
    r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());
}

TEST(TestServer, recv_requestvote_response_increase_votes_for_me)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());
    EXPECT_EQ(1, r.raft_get_current_term());

    r.raft_become_candidate();
    EXPECT_EQ(2, r.raft_get_current_term());
    EXPECT_EQ(1, r.raft_get_nvotes_for_me());

    msg_requestvote_response_t rvr = { 0 };
    rvr.term = 2;
    rvr.vote_granted = raft_request_vote::GRANTED;
    bmcl::Option<RaftError> e = r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_FALSE(e.isSome());
    EXPECT_EQ(2, r.raft_get_nvotes_for_me());
}

TEST(TestServer, recv_requestvote_response_must_be_candidate_to_receive)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());

    r.raft_become_leader();

    msg_requestvote_response_t rvr = { 0 };
    rvr.term = 1;
    rvr.vote_granted = raft_request_vote::GRANTED;
    r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_EQ(0, r.raft_get_nvotes_for_me());
}

/* Reply false if term < currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_false_if_term_less_than_current_term)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(2);

    /* term is less than current term */
    msg_requestvote_t rv = {0};
    rv.term = 1;
    auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
    EXPECT_TRUE(rvr.isOk());
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.unwrap().vote_granted);
}

TEST(TestServer, leader_recv_requestvote_does_not_step_down)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);
    r.raft_vote_for_nodeid(raft_node_id(1));
    r.raft_become_leader();
    EXPECT_TRUE(r.raft_is_leader());

    /* term is less than current term */
    msg_requestvote_t rv = {0};
    rv.term = 1;
    EXPECT_TRUE(r.raft_recv_requestvote(raft_node_id(2), rv).isOk());
    EXPECT_EQ(raft_node_id(1), r.raft_get_current_leader());
}

/* Reply true if term >= currentTerm (§5.1) */
TEST(TestServer, recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term)
{

    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);

    /* term is less than current term */
    msg_requestvote_t rv = {0};
    rv.term = 2;
    rv.last_log_idx = 1;
    auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
    EXPECT_TRUE(rvr.isOk());
    EXPECT_EQ(raft_request_vote::GRANTED, rvr.unwrap().vote_granted);
}

TEST(TestServer, recv_requestvote_reset_timeout)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);

    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_periodic(std::chrono::milliseconds(900));

    msg_requestvote_t rv = { 0 };
    rv.term = 2;
    rv.last_log_idx = 1;
    auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
    EXPECT_TRUE(rvr.isOk());
    EXPECT_EQ(raft_request_vote::GRANTED, rvr.unwrap().vote_granted);
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());
}

TEST(TestServer, recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_become_candidate();
    r.raft_set_current_term(1);
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());

    /* current term is less than term */
    msg_requestvote_t rv = { 0 };
    rv.candidate_id = raft_node_id(2);
    rv.term = 2;
    rv.last_log_idx = 1;
    EXPECT_TRUE(r.raft_recv_requestvote(raft_node_id(2), rv).isOk());
    EXPECT_TRUE(r.raft_is_follower());
    EXPECT_EQ(2, r.raft_get_current_term());
    EXPECT_EQ(raft_node_id(2), r.raft_get_voted_for());
}

TEST(TestServer, recv_requestvote_depends_on_candidate_id)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_become_candidate();
    r.raft_set_current_term(1);
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());

    /* current term is less than term */
    msg_requestvote_t rv = { 0 };
    rv.candidate_id = raft_node_id(3);
    rv.term = 2;
    rv.last_log_idx = 1;
    auto rvr = r.raft_recv_requestvote(bmcl::None, rv);
    EXPECT_TRUE(rvr.isOk());
    EXPECT_TRUE(r.raft_is_follower());
    EXPECT_EQ(2, r.raft_get_current_term());
    EXPECT_EQ(raft_node_id(3), r.raft_get_voted_for());
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (§5.2, §5.4) */
TEST(TestServer, recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(0));
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);

    /* vote for self */
    r.raft_vote_for_nodeid(raft_node_id(1));

    msg_requestvote_t rv = {0};
    rv.term = 1;
    rv.candidate_id = raft_node_id(1);
    rv.last_log_idx = 1;
    rv.last_log_term = 1;
    {
        auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
        EXPECT_TRUE(rvr.isOk());
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.unwrap().vote_granted);
    }

    /* vote for ID 0 */
    r.raft_vote_for_nodeid(raft_node_id(0));
    {
        auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
        EXPECT_TRUE(rvr.isOk());
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.unwrap().vote_granted);
    }
}

TEST(TestFollower, becomes_follower_is_follower)
{
    Raft r(raft_node_id(1), true);
    r.raft_become_follower();
    EXPECT_TRUE(r.raft_is_follower());
}

TEST(TestFollower, becomes_follower_does_not_clear_voted_for)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);

    r.raft_vote_for_nodeid(raft_node_id(1));
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
    r.raft_become_follower();
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
}

/* 5.1 */
TEST(TestFollower, recv_appendentries_reply_false_if_term_less_than_currentterm)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    /* no leader known at this point */
    EXPECT_FALSE(r.raft_get_current_leader().isSome());

    /* term is low */
    msg_appendentries_t ae = { 0 };
    ae.term = 1;

    /*  higher current term */
    r.raft_set_current_term(5);
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_FALSE(aer.unwrap().success);
    /* rejected appendentries doesn't change the current leader. */
    EXPECT_FALSE(r.raft_get_current_leader().isSome());
}

TEST(TestFollower, recv_appendentries_does_not_need_node)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    msg_appendentries_t ae = {0};
    ae.term = 1;
    auto aer = r.raft_recv_appendentries(bmcl::None, ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
}

/* TODO: check if test case is needed */
TEST(TestFollower, recv_appendentries_updates_currentterm_if_term_gt_currentterm)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    /*  older currentterm */
    r.raft_set_current_term(1);
    EXPECT_FALSE(r.raft_get_current_leader().isSome());

    /*  newer term for appendentry */
    msg_appendentries_t ae = {0};
    /* no prev log idx */
    ae.prev_log_idx = 0;
    ae.term = 2;

    /*  appendentry has newer term, so we change our currentterm */
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, aer.unwrap().term);
    /* term has been updated */
    EXPECT_EQ(2, r.raft_get_current_term());
    /* and leader has been updated */
    EXPECT_EQ(raft_node_id(2), r.raft_get_current_leader());
}

TEST(TestFollower, recv_appendentries_does_not_log_if_no_entries_are_specified)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);

    /*  log size s */
    EXPECT_EQ(0, r.raft_get_log_count());

    /* receive an appendentry with commit */
    msg_appendentries_t ae = {0};
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    ae.n_entries = 0;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.raft_get_log_count());
}

TEST(TestFollower, recv_appendentries_increases_log)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_entry_t ety = {0};
    char *str = "aaa";

    r.raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);

    /*  log size s */
    EXPECT_EQ(0, r.raft_get_log_count());

    /* receive an appendentry with commit */
    msg_appendentries_t ae = { 0 };
    ae.term = 3;
    ae.prev_log_term = 1;
    /* first appendentries msg */
    ae.prev_log_idx = 0;
    ae.leader_commit = 5;
    /* include one entry */
    memset(&ety, 0, sizeof(msg_entry_t));
    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    /* check that old terms are passed onto the log */
    ety.term = 2;
    ae.entries = &ety;
    ae.n_entries = 1;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.raft_get_log_count());
    bmcl::Option<const raft_entry_t&> log = r.raft_get_entry_from_idx(1);
    EXPECT_TRUE(log.isSome());
    EXPECT_EQ(2, log.unwrap().term);
}

/*  5.3 */
TEST(TestFollower, recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_entry_t ety = {0};
    char *str = "aaa";

    /* term is different from appendentries */
    r.raft_set_current_term(2);
    // TODO at log manually?

    /* log idx that server doesn't have */
    msg_appendentries_t ae = { 0 };
    ae.term = 2;
    ae.prev_log_idx = 1;
    /* prev_log_term is less than current term (ie. 2) */
    ae.prev_log_term = 1;
    /* include one entry */
    memset(&ety, 0, sizeof(msg_entry_t));
    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ae.entries = &ety;
    ae.n_entries = 1;

    /* trigger reply */
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    /* reply is false */
    EXPECT_FALSE(aer.unwrap().success);
}

static void __create_mock_entries_for_conflict_tests(Raft* r, char** strs)
{
    raft_entry_t ety = {0};
    bmcl::Option<const raft_entry_t&> ety_appended;

    /* increase log size */
    char *str1 = strs[0];
    ety.data.buf = str1;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    r->raft_append_entry(ety);
    EXPECT_EQ(1, r->raft_get_log_count());

    /* this log will be overwritten by a later appendentries */
    char *str2 = strs[1];
    ety.data.buf = str2;
    ety.data.len = 3;
    ety.id = 2;
    ety.term = 1;
    r->raft_append_entry(ety);
    EXPECT_EQ(2, r->raft_get_log_count());
    ety_appended = r->raft_get_entry_from_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str2, 3));

    /* this log will be overwritten by a later appendentries */
    char *str3 = strs[2];
    ety.data.buf = str3;
    ety.data.len = 3;
    ety.id = 3;
    ety.term = 1;
    r->raft_append_entry(ety);
    EXPECT_EQ(3, r->raft_get_log_count());
    ety_appended = r->raft_get_entry_from_idx(3);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str3, 3));
}

/* 5.3 */
TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    __create_mock_entries_for_conflict_tests(&r, strs);

    /* pass a appendentry that is newer  */
    msg_entry_t mety = {0};

    msg_appendentries_t ae = { 0 };
    ae.term = 2;
    /* entries from 2 onwards will be overwritten by this appendentries message */
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include one entry */
    memset(&mety, 0, sizeof(msg_entry_t));
    char *str4 = "444";
    mety.data.buf = str4;
    mety.data.len = 3;
    mety.id = 4;
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.raft_get_log_count());
    /* str1 is still there */

    bmcl::Option<const raft_entry_t&> ety_appended = r.raft_get_entry_from_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, strs[0], 3));
    /* str4 has overwritten the last 2 entries */

    ety_appended = r.raft_get_entry_from_idx(2);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str4, 3));
}

TEST(TestFollower, recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    __create_mock_entries_for_conflict_tests(&r, strs);

    /* pass a appendentry that is newer  */
    msg_entry_t mety = {0};

    msg_appendentries_t ae = { 0 };
    ae.term = 2;
    /* ALL append entries will be overwritten by this appendentries message */
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include one entry */
    memset(&mety, 0, sizeof(msg_entry_t));
    char *str4 = "444";
    mety.data.buf = str4;
    mety.data.len = 3;
    mety.id = 4;
    ae.entries = &mety;
    ae.n_entries = 1;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.raft_get_log_count());
    /* str1 is gone */
    bmcl::Option<const raft_entry_t&> ety_appended = r.raft_get_entry_from_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, str4, 3));
}

TEST(TestFollower, recv_appendentries_delete_entries_if_current_idx_greater_than_prev_log_idx)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_current_term(1);

    char* strs[] = {"111", "222", "333"};
    bmcl::Option<const raft_entry_t&> ety_appended;
    
    __create_mock_entries_for_conflict_tests(&r, strs);
    EXPECT_EQ(3, r.raft_get_log_count());

    msg_appendentries_t ae = { 0 };
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    msg_entry_t e[1];
    memset(&e, 0, sizeof(msg_entry_t) * 1);
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.raft_get_log_count());
    ety_appended = r.raft_get_entry_from_idx(1);
    EXPECT_TRUE(ety_appended.isSome());
    EXPECT_EQ(0, strncmp((const char*)ety_appended.unwrap().data.buf, strs[0], 3));
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

TEST(TestFollower, recv_appendentries_add_new_entries_not_already_in_log)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);


    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[2];
    memset(&e, 0, sizeof(msg_entry_t) * 2);
    e[0].id = 1;
    e[1].id = 2;
    ae.entries = e;
    ae.n_entries = 2;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.raft_get_log_count());
}

TEST(TestFollower, recv_appendentries_does_not_add_dupe_entries_already_in_log)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);

    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include 1 entry */
    msg_entry_t e[2];
    memset(&e, 0, sizeof(msg_entry_t) * 2);
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;

    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    /* still successful even when no raft_append_entry() happened! */
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
        EXPECT_EQ(1, r.raft_get_log_count());
    }

    /* lets get the server to append 2 now! */
    e[1].id = 2;
    ae.n_entries = 2;
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(2, r.raft_get_log_count());
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
TEST(TestFollower, recv_appendentries_set_commitidx_to_prevLogIdx)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    e[1].term = 1;
    e[1].id = 2;
    e[2].term = 1;
    e[2].id = 3;
    e[3].term = 1;
    e[3].id = 4;
    ae.entries = e;
    ae.n_entries = 4;

    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
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
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 4 because commitIDX is lower */
    EXPECT_EQ(4, r.raft_get_commit_idx());
}

TEST(TestFollower, recv_appendentries_set_commitidx_to_LeaderCommit)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    e[1].term = 1;
    e[1].id = 2;
    e[2].term = 1;
    e[2].id = 3;
    e[3].term = 1;
    e[3].id = 4;
    ae.entries = e;
    ae.n_entries = 4;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
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
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    /* set to 3 because leaderCommit is lower */
    EXPECT_EQ(3, r.raft_get_commit_idx());
}

TEST(TestFollower, recv_appendentries_failure_includes_current_idx)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_current_term(1);

    raft_entry_t ety = {0};
    ety.data.buf = "aaa";
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;
    r.raft_append_entry(ety);

    /* receive an appendentry with commit */
    msg_appendentries_t ae = { 0 };
    /* lower term means failure */
    ae.term = 0;
    ae.prev_log_term = 0;
    ae.prev_log_idx = 0;
    ae.leader_commit = 0;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_FALSE(aer.unwrap().success);
        EXPECT_EQ(1, aer.unwrap().current_idx);
    }

    /* try again with a higher current_idx */
    ety.id = 2;
    r.raft_append_entry(ety);
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_FALSE(aer.unwrap().success);
    EXPECT_EQ(2, aer.unwrap().current_idx);
}

TEST(TestFollower, becomes_candidate_when_election_timeout_occurs)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    /*  1 second election timeout */
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /*  1.001 seconds have passed */
    r.raft_periodic(std::chrono::milliseconds(1001));

    /* is a candidate now */
    EXPECT_TRUE(r.raft_is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, dont_grant_vote_if_candidate_has_a_less_complete_log)
{
    msg_requestvote_t rv;

    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 1;
    rv.candidate_id = raft_node_id(1);
    rv.last_log_idx = 1;
    rv.last_log_term = 1;

    r.raft_set_current_term(1);

    /* server's idx are more up-to-date */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    r.raft_append_entry(ety);
    ety.id = 101;
    ety.term = 2;
    r.raft_append_entry(ety);

    /* vote not granted */
    {
        auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
        EXPECT_TRUE(rvr.isOk());
        EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.unwrap().vote_granted);
    }

    /* approve vote, because last_log_term is higher */
    r.raft_set_current_term(2);
    memset(&rv, 0, sizeof(msg_requestvote_t));
    rv.term = 2;
    rv.candidate_id = raft_node_id(1);
    rv.last_log_idx = 1;
    rv.last_log_term = 3;
    {
        auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
        EXPECT_TRUE(rvr.isOk());
        EXPECT_EQ(raft_request_vote::GRANTED, rvr.unwrap().vote_granted);
    }
}

TEST(TestFollower, recv_appendentries_heartbeat_does_not_overwrite_logs)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[4];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
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
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    ae.entries = e;
    ae.n_entries = 4;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
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
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }

    EXPECT_EQ(5, r.raft_get_current_idx());
}

TEST(TestFollower, recv_appendentries_does_not_deleted_commited_entries)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    msg_appendentries_t ae = { 0 };
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 1;
    /* include entries */
    msg_entry_t e[5];
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 1;
    ae.entries = e;
    ae.n_entries = 1;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* Follow up AE. Node responded with success */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 4);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    ae.entries = e;
    ae.n_entries = 4;
    ae.leader_commit = 4;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
    }

    /* The server sends a follow up AE */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 5);
    e[0].term = 1;
    e[0].id = 2;
    e[1].term = 1;
    e[1].id = 3;
    e[2].term = 1;
    e[2].id = 4;
    e[3].term = 1;
    e[3].id = 5;
    e[4].term = 1;
    e[4].id = 6;
    ae.entries = e;
    ae.n_entries = 5;
    ae.leader_commit = 4;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.raft_get_current_idx());
    EXPECT_EQ(4, r.raft_get_commit_idx());

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 1;
    ae.prev_log_idx = 3;
    ae.prev_log_term = 1;
    /* include entries */
    memset(&e, 0, sizeof(msg_entry_t) * 5);
    e[0].id = 1;
    e[0].id = 5;
    e[1].term = 1;
    e[1].id = 6;
    e[2].term = 1;
    e[2].id = 7;
    ae.entries = e;
    ae.n_entries = 3;
    ae.leader_commit = 4;
    {
        auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
        EXPECT_TRUE(aer.isOk());
        EXPECT_TRUE(aer.unwrap().success);
    }
    EXPECT_EQ(6, r.raft_get_current_idx());
}

TEST(TestCandidate, becomes_candidate_is_candidate)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_become_candidate();
    EXPECT_TRUE(r.raft_is_candidate());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_increments_current_term)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);

    EXPECT_EQ(0, r.raft_get_current_term());
    r.raft_become_candidate();
    EXPECT_EQ(1, r.raft_get_current_term());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_votes_for_self)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);

    EXPECT_FALSE(r.raft_get_voted_for().isSome());
    r.raft_become_candidate();
    EXPECT_EQ(r.raft_get_my_nodeid(), r.raft_get_voted_for());
    EXPECT_EQ(1, r.raft_get_nvotes_for_me());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_resets_election_timeout)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), false, funcs);

    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    r.raft_periodic(std::chrono::milliseconds(900));
    EXPECT_EQ(900, r.raft_get_timeout_elapsed().count());

    r.raft_become_candidate();
    /* time is selected randomly */
    EXPECT_TRUE(r.raft_get_timeout_elapsed().count() < 1000);
}

TEST(TestFollower, recv_appendentries_resets_election_timeout)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    r.raft_periodic(std::chrono::milliseconds(900));

    msg_appendentries_t ae = {0};
    ae.term = 1;
    auto aer = r.raft_recv_appendentries(raft_node_id(1), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());
}

/* Candidate 5.2 */
TEST(TestFollower, becoming_candidate_requests_votes_from_other_servers)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = [&sender](Raft* raft, const RaftNode& node, const msg_requestvote_t& msg) {return sender.sender_requestvote(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));

    /* set term so we can check it gets included in the outbound message */
    r.raft_set_current_term(2);

    /* becoming candidate triggers vote requests */
    r.raft_become_candidate();

    bmcl::Option<msg_t> msg;
    msg_requestvote_t* rv;
    /* 2 nodes = 2 vote requests */
    msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_NE(2, rv->term);
    EXPECT_EQ(3, rv->term);

    /*  TODO: there should be more items */
    msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(3, rv->term);
}

/* Candidate 5.2 */
TEST(TestCandidate, election_timeout_and_no_leader_results_in_new_election)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));

    /* server wants to be leader, so becomes candidate */
    r.raft_become_candidate();
    EXPECT_EQ(1, r.raft_get_current_term());

    /* clock over (ie. 1000 + 1), causing new election */
    r.raft_periodic(std::chrono::milliseconds(1001));
    EXPECT_EQ(2, r.raft_get_current_term());

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
    msg_requestvote_response_t vr;

    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_add_node(raft_node_id(4));
    r.raft_add_node(raft_node_id(5));
    EXPECT_EQ(5, r.raft_get_num_nodes());

    /* vote for self */
    r.raft_become_candidate();
    EXPECT_EQ(1, r.raft_get_current_term());
    EXPECT_EQ(1, r.raft_get_nvotes_for_me());

    /* a vote for us */
    memset(&vr, 0, sizeof(msg_requestvote_response_t));
    vr.term = 1;
    vr.vote_granted = raft_request_vote::GRANTED;
    /* get one vote */
    r.raft_recv_requestvote_response(raft_node_id(2), vr);
    EXPECT_EQ(2, r.raft_get_nvotes_for_me());
    EXPECT_FALSE(r.raft_is_leader());

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    r.raft_recv_requestvote_response(raft_node_id(3), vr);
    EXPECT_TRUE(r.raft_is_leader());
}

/* Candidate 5.2 */
TEST(TestCandidate, will_not_respond_to_voterequest_if_it_has_already_voted)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_vote_for_nodeid(raft_node_id(1));

    msg_requestvote_t rv = {0};
    auto rvr = r.raft_recv_requestvote(raft_node_id(2), rv);
    EXPECT_TRUE(rvr.isOk());

    /* we've vote already, so won't respond with a vote granted... */
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, rvr.unwrap().vote_granted);
}

/* Candidate 5.2 */
TEST(TestCandidate, requestvote_includes_logidx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_requestvote = [&sender](Raft* raft, const RaftNode& node, const msg_requestvote_t& msg) {return sender.sender_requestvote(node, msg); };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);

    r.raft_set_current_term(5);
    /* 3 entries */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    r.raft_append_entry(ety);
    ety.id = 101;
    r.raft_append_entry(ety);
    ety.id = 102;
    ety.term = 3;
    r.raft_append_entry(ety);
    r.raft_send_requestvote(raft_node_id(2));

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    msg_requestvote_t* rv = msg->cast_to_requestvote().unwrapOr(nullptr);
    EXPECT_NE(nullptr, rv);
    EXPECT_EQ(3, rv->last_log_idx);
    EXPECT_EQ(5, rv->term);
    EXPECT_EQ(3, rv->last_log_term);
    EXPECT_EQ(raft_node_id(1), rv->candidate_id);
}

TEST(TestCandidate, recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_current_term(1);
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_vote_for_nodeid(bmcl::None);
    EXPECT_FALSE(r.raft_is_follower());
    EXPECT_FALSE(r.raft_get_current_leader().isSome());
    EXPECT_EQ(1, r.raft_get_current_term());

    msg_requestvote_response_t rvr = { 0 };
    rvr.term = 2;
    rvr.vote_granted = raft_request_vote::NOT_GRANTED;
    r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_TRUE(r.raft_is_follower());
    EXPECT_EQ(2, r.raft_get_current_term());
    EXPECT_FALSE(r.raft_get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_frm_leader_results_in_follower)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_vote_for_nodeid(bmcl::None);
    EXPECT_FALSE(r.raft_is_follower());
    EXPECT_FALSE(r.raft_get_current_leader().isSome());
    EXPECT_EQ(0, r.raft_get_current_term());

    /* receive recent appendentries */
    msg_appendentries_t ae = {0};
    ae.term = 1;
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());
    EXPECT_TRUE(r.raft_is_follower());
    /* after accepting a leader, it's available as the last known leader */
    EXPECT_EQ(raft_node_id(2), r.raft_get_current_leader());
    EXPECT_EQ(1, r.raft_get_current_term());
    EXPECT_FALSE(r.raft_get_voted_for().isSome());
}

/* Candidate 5.2 */
TEST(TestCandidate, recv_appendentries_from_same_term_results_in_step_down)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;
    funcs.send_requestvote = __raft_send_requestvote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_current_term(1);
    r.raft_become_candidate();
    EXPECT_FALSE(r.raft_is_follower());
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());

    msg_appendentries_t ae = {0};
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_FALSE(r.raft_is_candidate());

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
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
}

TEST(TestLeader, becomes_leader_is_leader)
{
    Raft r(raft_node_id(1), true);
    r.raft_become_leader();
    EXPECT_TRUE(r.raft_is_leader());
}

TEST(TestLeader, becomes_leader_does_not_clear_voted_for)
{
    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.persist_vote = __raft_persist_vote;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_vote_for_nodeid(raft_node_id(1));
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
    r.raft_become_leader();
    EXPECT_EQ(raft_node_id(1), r.raft_get_voted_for());
}

TEST(TestLeader, when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1)
{
    raft_cbs_t funcs = {0};
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));

    /* candidate to leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_become_leader();

    int i;
    for (i = 2; i <= 3; i++)
    {
        bmcl::Option<RaftNode&> p = r.raft_get_node(raft_node_id(i));
        EXPECT_TRUE(p.isSome());
        EXPECT_EQ(r.raft_get_current_idx() + 1, p->raft_node_get_next_idx());
    }
}

/* 5.2 */
TEST(TestLeader, when_it_becomes_a_leader_sends_empty_appendentries)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));

    /* candidate to leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_become_leader();

    /* receive appendentries messages for both nodes */
    bmcl::Option<msg_t> msg;
    msg_appendentries_t* ae;

    msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);

    msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
TEST(TestLeader, responds_to_entry_msg_when_entry_is_committed)
{
    raft_cbs_t funcs = {0};
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    /* I am the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    EXPECT_EQ(0, r.raft_get_log_count());

    /* entry message */
    msg_entry_t ety = {0};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(ety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.raft_get_log_count());

    /* trigger response through commit */
    r.raft_apply_entry();
}

void TestRaft_non_leader_recv_entry_msg_fails()
{
    Raft r(raft_node_id(1), true);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_FOLLOWER);

    /* entry message */
    msg_entry_t ety = {0};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(ety);
    EXPECT_TRUE(cr.isErr());
    EXPECT_EQ(RaftError::NotLeader, cr.unwrapErr());
}

/* 5.3 */
TEST(TestLeader, sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));

    /* i'm leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    bmcl::Option<RaftNode&> p = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(p.isSome());
    p->raft_node_set_next_idx(4);

    /* receive appendentries messages */
    r.raft_send_appendentries(p.unwrap());

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
}

TEST(TestLeader, sends_appendentries_with_leader_commit)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };

    r.raft_set_callbacks(funcs);
    r.raft_add_node(raft_node_id(2));

    /* i'm leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    int i;

    for (i=0; i<10; i++)
    {
        raft_entry_t ety = {0};
        ety.term = 1;
        ety.id = 1;
        ety.data.buf = "aaa";
        ety.data.len = 3;
        r.raft_append_entry(ety);
    }

    r.raft_set_commit_idx(10);

    /* receive appendentries messages */
    r.raft_send_appendentries(raft_node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
        EXPECT_EQ(10, ae->leader_commit);
    }
}

TEST(TestLeader, sends_appendentries_with_prevLogIdx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };

    r.raft_set_callbacks(funcs);
    r.raft_add_node(raft_node_id(2));

    /* i'm leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    /* receive appendentries messages */
    r.raft_send_appendentries(raft_node_id(2));

    bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
    EXPECT_TRUE(msg.isSome());
    msg_appendentries_t* ae = msg->cast_to_appendentries().unwrapOr(nullptr);
    EXPECT_NE(nullptr, ae);
    EXPECT_EQ(0, ae->prev_log_idx);

    bmcl::Option<RaftNode&> n = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(n.isSome());

    /* add 1 entry */
    /* receive appendentries messages */
    raft_entry_t ety = {0};
    ety.term = 2;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";

    r.raft_append_entry(ety);
    n->raft_node_set_next_idx(1);
    r.raft_send_appendentries(n.unwrap());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
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
    n->raft_node_set_next_idx(2);
    r.raft_send_appendentries(raft_node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_idx);
    }
}

TEST(TestLeader, sends_appendentries_when_node_has_next_idx_of_0)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };

    r.raft_set_callbacks(funcs);
    r.raft_add_node(raft_node_id(2));

    /* i'm leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    /* receive appendentries messages */
    r.raft_send_appendentries(raft_node_id(2));
    msg_appendentries_t*  ae;;
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* add an entry */
    /* receive appendentries messages */
    bmcl::Option<RaftNode&> n = r.raft_get_node(raft_node_id(2));
    n->raft_node_set_next_idx(1);
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 100;
    ety.data.len = 4;
    ety.data.buf = (unsigned char*)"aaa";
    r.raft_append_entry(ety);
    r.raft_send_appendentries(n.unwrap());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(0, ae->prev_log_idx);
    }
}

/* 5.3 */
TEST(TestLeader, retries_appendentries_with_decremented_NextIdx_log_inconsistency)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = {0};
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));

    /* i'm leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    /* receive appendentries messages */
    r.raft_send_appendentries(raft_node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
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
    raft_cbs_t funcs = {0};
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    EXPECT_EQ(0, r.raft_get_log_count());

    msg_entry_t ety = {0};
    ety.id = 1;
    ety.data.buf = "entry";
    ety.data.len = strlen("entry");

    auto cr = r.raft_recv_entry(ety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(1, r.raft_get_log_count());
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
    ety.data = "entry";
    ety.data.len = strlen("entry");

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
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_add_node(raft_node_id(4));
    r.raft_add_node(raft_node_id(5));
    r.raft_set_callbacks(funcs);

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */
    r.raft_set_last_applied_idx(0);

    /* append entries - we need two */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);
    ety.id = 2;
    r.raft_append_entry(ety);
    ety.id = 3;
    r.raft_append_entry(ety);

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));

    /* receive mock success responses */
    msg_appendentries_response_t aer = {0};
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_recv_appendentries_response(raft_node_id(3), aer);
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(1, r.raft_get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.raft_get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */

    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));

    /* receive mock success responses */
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 2;
    aer.first_idx = 2;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(1, r.raft_get_commit_idx());
    r.raft_recv_appendentries_response(raft_node_id(3), aer);
    /* leader will now have majority followers who have appended this log */
    EXPECT_EQ(2, r.raft_get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.raft_get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_add_non_voting_node(raft_node_id(4));
    r.raft_add_non_voting_node(raft_node_id(5));

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */
    r.raft_set_last_applied_idx(0);

    /* append entries - we need two */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));

    /* receive mock success responses */
    msg_appendentries_response_t aer = { 0 };
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(1, r.raft_get_commit_idx());
    /* leader will now have majority followers who have appended this log */
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.raft_get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_duplicate_does_not_decrement_match_idx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_callbacks(funcs);

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */
    r.raft_set_last_applied_idx(0);

    /* append entries - we need two */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);
    ety.id = 2;
    r.raft_append_entry(ety);
    ety.id = 3;
    r.raft_append_entry(ety);

    msg_appendentries_response_t aer = { 0 };

    /* receive msg 1 */
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(1, r.raft_get_node(raft_node_id(2))->raft_node_get_match_idx());

    /* receive msg 2 */
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 2;
    aer.first_idx = 2;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(2, r.raft_get_node(raft_node_id(2))->raft_node_get_match_idx());

    /* receive msg 1 - because of duplication ie. unreliable network */
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(2, r.raft_get_node(raft_node_id(2))->raft_node_get_match_idx());
}

TEST(TestLeader, recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.applylog = __raft_applylog;
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_add_node(raft_node_id(4));
    r.raft_add_node(raft_node_id(5));
    r.raft_set_callbacks(funcs);

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(2);
    r.raft_set_commit_idx(0);
    r.raft_set_last_applied_idx(0);

    /* append entries - we need two */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);
    ety.id = 2;
    r.raft_append_entry(ety);
    ety.term = 2;
    ety.id = 3;
    r.raft_append_entry(ety);

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));
    /* receive mock success responses */
    msg_appendentries_response_t aer = { 0 };
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_recv_appendentries_response(raft_node_id(3), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.raft_get_last_applied_idx());

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 2;
    aer.first_idx = 2;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_recv_appendentries_response(raft_node_id(3), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(0, r.raft_get_last_applied_idx());

    /* THIRD entry log application */
    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    aer.term = 2;
    aer.success = true;
    aer.current_idx = 3;
    aer.first_idx = 3;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(0, r.raft_get_commit_idx());
    r.raft_recv_appendentries_response(raft_node_id(3), aer);
    EXPECT_EQ(3, r.raft_get_commit_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(1, r.raft_get_last_applied_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(2, r.raft_get_last_applied_idx());
    r.raft_periodic(std::chrono::milliseconds(1));
    EXPECT_EQ(3, r.raft_get_last_applied_idx());
}

TEST(TestLeader, recv_appendentries_response_jumps_to_lower_next_idx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_set_callbacks(funcs);

    r.raft_set_current_term(2);
    r.raft_set_commit_idx(0);

    /* append entries */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);
    ety.term = 2;
    ety.id = 2;
    r.raft_append_entry(ety);
    ety.term = 3;
    ety.id = 3;
    r.raft_append_entry(ety);
    ety.term = 4;
    ety.id = 4;
    r.raft_append_entry(ety);

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    r.raft_become_leader();
    bmcl::Option<RaftNode&> node = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(node.isSome());
    EXPECT_EQ(5, node->raft_node_get_next_idx());

    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    msg_appendentries_response_t aer = { 0 };
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 2;
    aer.success = false;
    aer.current_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(2, node->raft_node_get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(1, ae->prev_log_term);
        EXPECT_EQ(1, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.sender_poll_msg_data().isSome());
}

TEST(TestLeader, recv_appendentries_response_decrements_to_lower_next_idx)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_set_callbacks(funcs);

    r.raft_set_current_term(2);
    r.raft_set_commit_idx(0);

    /* append entries */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);
    ety.term = 2;
    ety.id = 2;
    r.raft_append_entry(ety);
    ety.term = 3;
    ety.id = 3;
    r.raft_append_entry(ety);
    ety.term = 4;
    ety.id = 4;
    r.raft_append_entry(ety);

    msg_appendentries_t* ae;

    /* become leader sets next_idx to current_idx */
    r.raft_become_leader();
    bmcl::Option<RaftNode&> node = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(node.isSome());
    EXPECT_EQ(5, node->raft_node_get_next_idx());
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(4, ae->prev_log_term);
        EXPECT_EQ(4, ae->prev_log_idx);
    }

    /* receive mock success responses */
    msg_appendentries_response_t aer = {0};
    aer.term = 2;
    aer.success = false;
    aer.current_idx = 4;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(4, node->raft_node_get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(3, ae->prev_log_term);
        EXPECT_EQ(3, ae->prev_log_idx);
    }

    /* receive mock success responses */
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 2;
    aer.success = false;
    aer.current_idx = 4;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(3, node->raft_node_get_next_idx());

    /* see if new appendentries have appropriate values */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);

        EXPECT_EQ(2, ae->prev_log_term);
        EXPECT_EQ(2, ae->prev_log_idx);
    }

    EXPECT_FALSE(sender.sender_poll_msg_data().isSome());
}

TEST(TestLeader, recv_appendentries_response_retry_only_if_leader)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_callbacks(funcs);

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);
    /* the last applied idx will became 1, and then 2 */
    r.raft_set_last_applied_idx(0);

    /* append entries - we need two */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);

    r.raft_send_appendentries(raft_node_id(2));
    r.raft_send_appendentries(raft_node_id(3));

    EXPECT_TRUE(sender.sender_poll_msg_data().isSome());
    EXPECT_TRUE(sender.sender_poll_msg_data().isSome());

    r.raft_become_follower();

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    EXPECT_TRUE(r.raft_recv_appendentries_response(raft_node_id(2), aer).isSome());
    EXPECT_FALSE(sender.sender_poll_msg_data().isSome());
}

TEST(TestLeader, recv_appendentries_response_without_node_fails)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 0;
    aer.first_idx = 0;
    EXPECT_TRUE(r.raft_recv_appendentries_response(bmcl::None, aer).isSome());
}

TEST(TestLeader, recv_entry_resets_election_timeout)
{
    Raft r(raft_node_id(1), true);
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);

    r.raft_periodic(std::chrono::milliseconds(900));

    /* entry message */
    msg_entry_t mety = {0};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(mety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());
}

TEST(TestLeader, recv_entry_is_committed_returns_0_if_not_committed)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);

    /* entry message */
    msg_entry_t mety = {0};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(mety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.raft_msg_entry_response_committed(cr.unwrap()));

    r.raft_set_commit_idx(1);
    EXPECT_EQ(1, r.raft_msg_entry_response_committed(cr.unwrap()));
}

TEST(TestLeader, recv_entry_is_committed_returns_neg_1_if_invalidated)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = __raft_send_appendentries;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);

    /* entry message */
    msg_entry_t mety = {0};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(mety);
    EXPECT_TRUE(cr.isOk());
    EXPECT_EQ(0, r.raft_msg_entry_response_committed(cr.unwrap()));
    EXPECT_EQ(1, cr.unwrap().term);
    EXPECT_EQ(1, cr.unwrap().idx);
    EXPECT_EQ(1, r.raft_get_current_idx());
    EXPECT_EQ(0, r.raft_get_commit_idx());

    /* append entry that invalidates entry message */
    msg_appendentries_t ae = {0};
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    msg_entry_t e[1] = {0};
    e[0].term = 2;
    e[0].id = 999;
    e[0].data.buf = "aaa";
    e[0].data.len = 3;

    ae.entries = e;
    ae.n_entries = 1;
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(aer.unwrap().success);
    EXPECT_EQ(1, r.raft_get_current_idx());
    EXPECT_EQ(1, r.raft_get_commit_idx());
    EXPECT_EQ(-1, r.raft_msg_entry_response_committed(cr.unwrap()));
}

TEST(TestLeader, recv_entry_does_not_send_new_appendentries_to_slow_nodes)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);

    /* make the node slow */
    r.raft_get_node(raft_node_id(2))->raft_node_set_next_idx(1);

    /* append entries */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);

    /* entry message */
    msg_entry_t mety = {0};
    mety.id = 1;
    mety.data.buf = "entry";
    mety.data.len = strlen("entry");

    /* receive entry */
    auto cr = r.raft_recv_entry(mety);
    EXPECT_TRUE(cr.isOk());

    /* check if the slow node got sent this appendentries */
    {
        bmcl::Option<msg_t> msg = sender.sender_poll_msg_data();
        EXPECT_FALSE(msg.isSome());
    }
}

TEST(TestLeader, recv_appendentries_response_failure_does_not_set_node_nextid_to_0)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_set_callbacks(funcs);

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);
    r.raft_set_commit_idx(0);

    /* append entries */
    raft_entry_t ety = {0};
    ety.term = 1;
    ety.id = 1;
    ety.data.buf = "aaaa";
    ety.data.len = 4;
    r.raft_append_entry(ety);

    /* send appendentries -
     * server will be waiting for response */
    r.raft_send_appendentries(raft_node_id(2));

    /* receive mock success response */
    msg_appendentries_response_t aer;
    memset(&aer, 0, sizeof(msg_appendentries_response_t));
    aer.term = 1;
    aer.success = false;
    aer.current_idx = 0;
    aer.first_idx = 0;
    bmcl::Option<RaftNode&> p = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(p.isSome());
    r.raft_recv_appendentries_response(p->raft_node_get_id(), aer);
    EXPECT_EQ(1, p->raft_node_get_next_idx());
    r.raft_recv_appendentries_response(p->raft_node_get_id(), aer);
    EXPECT_EQ(1, p->raft_node_get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_increment_idx_of_node)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(1);

    bmcl::Option<RaftNode&> p = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->raft_node_get_next_idx());

    /* receive mock success responses */
    msg_appendentries_response_t aer;
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 0;
    aer.first_idx = 0;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(1, p->raft_node_get_next_idx());
}

TEST(TestLeader, recv_appendentries_response_drop_message_if_term_is_old)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_term = __raft_persist_term;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));

    /* I'm the leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(2);

    bmcl::Option<RaftNode&> p = r.raft_get_node(raft_node_id(2));
    EXPECT_TRUE(p.isSome());
    EXPECT_EQ(1, p->raft_node_get_next_idx());

    /* receive OLD mock success responses */
    msg_appendentries_response_t aer;
    aer.term = 1;
    aer.success = true;
    aer.current_idx = 1;
    aer.first_idx = 1;
    r.raft_recv_appendentries_response(raft_node_id(2), aer);
    EXPECT_EQ(1, p->raft_node_get_next_idx());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(5);
    /* check that node 0 considers itself the leader */
    EXPECT_TRUE(r.raft_is_leader());
    EXPECT_EQ(raft_node_id(1), r.raft_get_current_leader());

    msg_appendentries_t ae = {0};
    ae.term = 6;
    ae.prev_log_idx = 6;
    ae.prev_log_term = 5;
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    /* after more recent appendentries from node 1, node 0 should
     * consider node 1 the leader. */
    EXPECT_TRUE(r.raft_is_follower());
    EXPECT_EQ(raft_node_id(1), r.raft_get_current_leader());
}

TEST(TestLeader, recv_appendentries_steps_down_if_newer_term)
{
    raft_cbs_t funcs = {0};
    funcs.persist_term = __raft_persist_term;

    Raft r(raft_node_id(1), true, funcs);
    r.raft_add_node(raft_node_id(2));

    r.raft_set_state(raft_state_e::RAFT_STATE_LEADER);
    r.raft_set_current_term(5);

    msg_appendentries_t ae = {0};
    ae.term = 6;
    ae.prev_log_idx = 5;
    ae.prev_log_term = 5;
    auto aer = r.raft_recv_appendentries(raft_node_id(2), ae);
    EXPECT_TRUE(aer.isOk());

    EXPECT_TRUE(r.raft_is_follower());
}

TEST(TestLeader, sends_empty_appendentries_every_request_timeout)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    /* candidate to leader */
    r.raft_set_state(raft_state_e::RAFT_STATE_CANDIDATE);
    r.raft_become_leader();

    bmcl::Option<msg_t> msg;
    msg_appendentries_t* ae;

    /* receive appendentries messages for both nodes */
    {
        msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.sender_poll_msg_data();
        EXPECT_TRUE(msg.isSome());
        ae = msg->cast_to_appendentries().unwrapOr(nullptr);
        EXPECT_NE(nullptr, ae);
    }
    {
        msg = sender.sender_poll_msg_data();
        EXPECT_FALSE(msg.isSome());
    }

    /* force request timeout */
    r.raft_periodic(std::chrono::milliseconds(501));
    {
        msg = sender.sender_poll_msg_data();
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
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_vote = __raft_persist_vote;
    funcs.persist_term = __raft_persist_term;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    r.raft_election_start();

    msg_requestvote_response_t rvr = {0};
    rvr.term = 1;
    rvr.vote_granted = raft_request_vote::GRANTED;
    r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_TRUE(r.raft_is_leader());

    /* receive request vote from node 3 */
    msg_requestvote_t rv = {0};
    rv.term = 1;
    auto opt = r.raft_recv_requestvote(raft_node_id(3), rv);
    EXPECT_TRUE(opt.isOk());
    EXPECT_EQ(raft_request_vote::NOT_GRANTED, opt.unwrap().vote_granted);
}

TEST(TestLeader, recv_requestvote_responds_with_granting_if_term_is_higher)
{
    Raft r(raft_node_id(1), true);
    Sender sender(&r);

    raft_cbs_t funcs = { 0 };
    funcs.persist_vote = __raft_persist_vote;
    funcs.persist_term = __raft_persist_term;
    funcs.send_requestvote = __raft_send_requestvote;
    funcs.send_appendentries = [&sender](Raft * raft, const RaftNode & node, const msg_appendentries_t& msg) { return sender.sender_appendentries(node, msg); };
    r.raft_set_callbacks(funcs);

    r.raft_add_node(raft_node_id(2));
    r.raft_add_node(raft_node_id(3));
    r.raft_set_election_timeout(std::chrono::milliseconds(1000));
    r.raft_set_request_timeout(std::chrono::milliseconds(500));
    EXPECT_EQ(0, r.raft_get_timeout_elapsed().count());

    r.raft_election_start();

    msg_requestvote_response_t rvr = {0};
    rvr.term = 1;
    rvr.vote_granted = raft_request_vote::GRANTED;
    r.raft_recv_requestvote_response(raft_node_id(2), rvr);
    EXPECT_TRUE(r.raft_is_leader());

    /* receive request vote from node 3 */
    msg_requestvote_t rv = {0};
    rv.term = 2;
    EXPECT_TRUE(r.raft_recv_requestvote(raft_node_id(3), rv).isOk());
    EXPECT_TRUE(r.raft_is_follower());
}
