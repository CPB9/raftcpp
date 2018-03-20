#include <memory>
#include <gtest/gtest.h>
#include "raft/Raft.h"
#include "raft/Committer.h"
#include "mock_send_functions.h"

using namespace raft;

TEST(TestScenario, leader_appears)
{
    std::vector<std::shared_ptr<raft::Server>> servers;
    std::vector<std::shared_ptr<raft::MemStorage>> storages;

    Exchanger sender;

    std::vector<raft::NodeId> set = {NodeId(1), NodeId(2), NodeId(3)};

    for (const NodeId& i : set)
    {
        storages.emplace_back(std::make_shared<raft::MemStorage>());
        servers.emplace_back(std::make_shared<raft::Server>(i, set, [](Index, Entry) {return bmcl::None; }, storages.back().get(), nullptr));
        raft::Server& rx = *servers.back();
        rx.timer().set_timeout(Time(100), 5);
        sender.add(&rx);
    }

    /* NOTE: important for 1st node to send vote request before others */
    //r[0].raft_periodic(Time(1000));

    std::size_t i;
    for (i = 0; i < 20; i++)
    {
        std::cout << i << " " << i*100;
one_more_time:

        for (const auto& j : servers)
        {
            std::cout << " (" << j->get_current_term() << ", " << (int)j->get_state() << ")";
            sender.poll_msgs(j->nodes().get_my_id());

        }
        std::cout << std::endl;

        for (const auto& j : servers)
        {
            if (sender.msgs_available(j->nodes().get_my_id()))
                goto one_more_time;
        }

        for (const auto& j : servers)
        {
            j->tick(Time(100));
        }
    }

    int leaders = 0;
    for (const auto& j : servers)
    {
        if (j->is_leader())
            leaders += 1;
    }

    EXPECT_EQ(1, leaders);
}
