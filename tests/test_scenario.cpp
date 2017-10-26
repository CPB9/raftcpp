#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"

TEST(TestScenario, leader_appears)
{
    std::vector<Raft> r;
    std::vector<Sender> sender;

    r.resize(3);
    for (std::size_t i = 0; i < 3; ++i)
    {
        sender.emplace_back(&r[i]);
    }

    for (std::size_t i = 0; i < 3; ++i)
    {
        r[i].raft_set_election_timeout(std::chrono::milliseconds(500));
        raft_cbs_t funcs = { 0 };
        Sender* s = &sender[i];
        funcs.send_requestvote = [s](const Raft* raft, const RaftNode& node, const msg_requestvote_t& msg) { return s->sender_requestvote(node, msg); };
        funcs.send_appendentries = [s](const Raft* raft, const RaftNode& node, const msg_appendentries_t& msg) { return s->sender_appendentries(node, msg); };
        funcs.persist_term = [s](Raft * raft, int node) -> bmcl::Option<RaftError> { return bmcl::None; };
        funcs.persist_vote = [s](Raft * raft, int node) -> bmcl::Option<RaftError> { return bmcl::None; };
        r[i].raft_set_callbacks(funcs);

        for (const auto& j : sender)
        {
            r[i].raft_add_node(&sender[0], raft_node_id(1), i==0);
            r[i].raft_add_node(&sender[1], raft_node_id(2), i==1);
            r[i].raft_add_node(&sender[2], raft_node_id(3), i==2);
        }
    }

    /* NOTE: important for 1st node to send vote request before others */
    r[0].raft_periodic(std::chrono::milliseconds(1000));

    for (std::size_t i = 0; i < 20; i++)
    {
one_more_time:

        for (std::size_t j = 0; j < 3; j++)
            sender[j].sender_poll_msgs();

        for (std::size_t j = 0; j < 3; j++)
            if (sender[j].sender_msgs_available())
                goto one_more_time;

        for (std::size_t j = 0; j < 3; j++)
            r[j].raft_periodic(std::chrono::milliseconds(100));
    }

    int leaders = 0;
    for (std::size_t j = 0; j < 3; j++)
        if (r[j].raft_is_leader())
            leaders += 1;

    EXPECT_NE(0, leaders);
    EXPECT_EQ(1, leaders);
}
