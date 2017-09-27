#include <stdio.h>
#include "raft/Raft.h"
#include "mock_send_functions.h"


static bmcl::Option<RaftError> __append_msg(sender_t* me, const void* data, int len, raft_message_type_e type, const RaftNode& node)
{
    assert(me->raft->raft_get_my_nodeid().isSome());
    me->outbox.emplace_back(data, len, type, me->raft->raft_get_my_nodeid().unwrap());

    /* give to peer */
    sender_t* peer = (sender_t*)node.raft_node_get_udata();
    if (peer)
    {
        msg_t m2 = me->outbox.back();
        auto n = peer->raft->raft_get_my_node();
        assert(n.isSome());
        m2.sender = n.unwrap().raft_node_get_id();
        peer->inbox.push_back(m2);
    }

    return bmcl::None;
}

bmcl::Option<RaftError> Sender::sender_requestvote(const RaftNode& node, const msg_requestvote_t& msg)
{
    return __append_msg(&me, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE, node);
}

bmcl::Option<RaftError> Sender::sender_requestvote_response(const RaftNode& node, const msg_requestvote_response_t& msg)
{
    return __append_msg(&me, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE, node);
}

bmcl::Option<RaftError> Sender::sender_appendentries(const RaftNode& node, const msg_appendentries_t& msg)
{
    msg_entry_t* entries = (msg_entry_t*)calloc(1, sizeof(msg_entry_t) * msg.n_entries);
    memcpy(entries, msg.entries, sizeof(msg_entry_t) * msg.n_entries);
    msg_appendentries_t tmp = msg;
    tmp.entries = entries;
    return __append_msg(&me, &tmp, sizeof(tmp), raft_message_type_e::RAFT_MSG_APPENDENTRIES, node);
}

bmcl::Option<RaftError> Sender::sender_appendentries_response(const RaftNode& node, const msg_appendentries_response_t& msg)
{
    return __append_msg(&me, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE, node);
}

bmcl::Option<RaftError> Sender::sender_entries_response(const RaftNode& node, const msg_entry_response_t& msg)
{
    return __append_msg(&me, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE, node);
}

bmcl::Option<msg_t> Sender::sender_poll_msg_data()
{
    if (me.outbox.empty())
        return bmcl::None;
    msg_t m = me.outbox.front();
    me.outbox.pop_front();
    return m;
}

bool Sender::sender_msgs_available()
{
    return !me.inbox.empty();
}

void Sender::sender_poll_msgs()
{
    std::cout << "start " << me.inbox.size();
    for(const msg_t& m: me.inbox)
    {
        std::cout << "  in " << me.inbox.size() << std::endl;;
        switch (m.type)
        {
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES:
        {
            EXPECT_EQ(sizeof(msg_appendentries_t), m.data.size());
            auto r = me.raft->raft_recv_appendentries(m.sender, *(msg_appendentries_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_appendentries_response_t response = r.unwrap();
            __append_msg(&me, &response, sizeof(response), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE, me.raft->raft_get_my_node().unwrap());
        }
        break;
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE:
            EXPECT_EQ(sizeof(msg_appendentries_response_t), m.data.size());
            me.raft->raft_recv_appendentries_response(m.sender, *(msg_appendentries_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE:
        {
            EXPECT_EQ(sizeof(msg_requestvote_t), m.data.size());
            auto r = me.raft->raft_recv_requestvote(m.sender, *(msg_requestvote_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_requestvote_response_t response = r.unwrap();
            __append_msg(&me, &response, sizeof(response), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE, me.raft->raft_get_my_node().unwrap());
        }
        break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE:
            EXPECT_EQ(sizeof(msg_requestvote_response_t), m.data.size());
            me.raft->raft_recv_requestvote_response(m.sender, *(msg_requestvote_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_ENTRY:
        {
            EXPECT_EQ(sizeof(msg_entry_t), m.data.size());
            auto r = me.raft->raft_recv_entry(*(msg_entry_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_entry_response_t response = r.unwrap();
            __append_msg(&me, (msg_entry_t *)&response, sizeof(response), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE, me.raft->raft_get_my_node().unwrap());
        }
        break;

        case raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE:
#if 0
            raft_recv_entry_response(me->raft, m->sender, m->data);
#endif
            break;
        }
        std::cout << " " << me.inbox.size() << std::endl;
    }
    std::cout << " end" << std::endl;
    me.inbox.clear();
}
