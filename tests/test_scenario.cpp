#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Log.h"
#include "mock_send_functions.h"

using namespace raft;

TEST(TestScenario, leader_appears)
{
    std::vector<raft::Server> r;
    Exchanger sender;
    Saver saver;

    const std::size_t Count = 3;
    for (std::size_t i = 0; i < Count; ++i)
    {
        r.emplace_back(raft::Server(raft::NodeId(i), true, nullptr, &saver));
        raft::Server& rx = r.back();

        for(std::size_t j = 1; j < Count; ++j)
        {
            rx.nodes().add_node(raft::NodeId((i + j) % Count));
        }
        rx.timer().set_election_timeout(std::chrono::milliseconds(500));
    }

    for (std::size_t i = 0; i < 3; ++i)
    {
        sender.add(&r[i]);
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
            std::cout << " (" << r[j].get_current_term() << ", "<< (int)r[j].get_state()<< ")";
            sender.poll_msgs(raft::NodeId(j));
        }
        std::cout << std::endl;

        for (std::size_t j = 0; j < 3; j++)
            if (sender.msgs_available(raft::NodeId(j)))
                goto one_more_time;

        for (std::size_t j = 0; j < 3; j++)
            r[j].raft_periodic(std::chrono::milliseconds(100));
    }

    int leaders = 0;
    for (std::size_t j = 0; j < 3; j++)
        if (r[j].is_leader())
            leaders += 1;

    EXPECT_EQ(1, leaders);
}
