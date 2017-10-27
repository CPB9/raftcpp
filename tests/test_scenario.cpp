#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"

TEST(TestScenario, leader_appears)
{
    std::vector<Raft> r;
    Sender sender;

    const std::size_t Count = 3;
    for (std::size_t i = 0; i < Count; ++i)
    {
        r.emplace_back(Raft(raft_node_id(i), true));
        Raft& rx = r.back();

        for(std::size_t j = 1; j < Count; ++j)
        {
            rx.raft_add_node(raft_node_id((i + j) % Count));
        }
        rx.raft_set_election_timeout(std::chrono::milliseconds(500));
    }

    for (std::size_t i = 0; i < 3; ++i)
    {
        sender.add(&r[i]);
    }

    for (std::size_t i = 0; i < 3; ++i)
    {
        raft_cbs_t funcs = { 0 };
        funcs.send_requestvote = [&sender](const Raft* raft, const RaftNode& node, const msg_requestvote_t& msg) { return sender.sender_requestvote(raft, node, msg); };
        funcs.send_appendentries = [&sender](const Raft* raft, const RaftNode& node, const msg_appendentries_t& msg) { return sender.sender_appendentries(raft, node, msg); };
        funcs.persist_term = [&sender](Raft * raft, int node) -> bmcl::Option<RaftError> { return bmcl::None; };
        funcs.persist_vote = [&sender](Raft * raft, int node) -> bmcl::Option<RaftError> { return bmcl::None; };
        r[i].raft_set_callbacks(funcs);

    }

    /* NOTE: important for 1st node to send vote request before others */
    //r[0].raft_periodic(std::chrono::milliseconds(1000));

    std::size_t i;
    for (i = 0; i < 20; i++)
    {
        std::cout << i << " " << i*100;
one_more_time:

        for (std::size_t j = 0; j < 3; j++)
        {
            std::cout << " (" << r[j].raft_get_current_term() << ", "<< (int)r[j].raft_get_state()<< ")";
            sender.sender_poll_msgs(raft_node_id(j));
        }
        std::cout << std::endl;

        for (std::size_t j = 0; j < 3; j++)
            if (sender.sender_msgs_available(raft_node_id(j)))
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
