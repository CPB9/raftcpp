#include <stdio.h>
#include "raft/Raft.h"
#include "mock_send_functions.h"


bmcl::Option<raft::Error> Sender::__append_msg(const raft::node_id& from, const raft::node_id& to, const void* data, int len, raft_message_type_e type)
{
    auto f = _servers[from].raft;
    auto peer = _servers[to].raft;

    assert(_servers[from].raft);

    _servers[from].outbox.emplace_back(data, len, type, f->nodes().get_my_id());

    /* give to peer */
    if (peer)
    {
        msg_t m2 = _servers[from].outbox.back();
        m2.sender = from;
        _servers[to].inbox.push_back(m2);
    }

    return bmcl::None;
}

bmcl::Option<raft::Error> Sender::sender_requestvote(const raft::Server* raft, const msg_requestvote_t& msg)
{
    //return __append_msg(from->nodes().get_my_id(), to.get_id(), &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE);

    for (const auto& to : _servers)
    {
        if (to.first != raft->nodes().get_my_id())
            __append_msg(raft->nodes().get_my_id(), to.first, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE);
    }
    return bmcl::None;
}

bmcl::Option<raft::Error> Sender::sender_requestvote_response(const raft::node_id& from, const raft::node_id& to, const msg_requestvote_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
}

bmcl::Option<raft::Error> Sender::sender_appendentries(const raft::Server * from, const raft::node_id & to, const msg_appendentries_t& msg)
{
    msg_entry_t* entries = (msg_entry_t*)calloc(1, sizeof(msg_entry_t) * msg.n_entries);
    memcpy(entries, msg.entries, sizeof(msg_entry_t) * msg.n_entries);
    msg_appendentries_t tmp = msg;
    tmp.entries = entries;
    return __append_msg(from->nodes().get_my_id(), to, &tmp, sizeof(tmp), raft_message_type_e::RAFT_MSG_APPENDENTRIES);
}

bmcl::Option<raft::Error> Sender::sender_appendentries_response(const raft::node_id& from, const raft::node_id& to, const msg_appendentries_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
}

bmcl::Option<raft::Error> Sender::sender_entries_response(const raft::node_id& from, const raft::node_id& to, const msg_entry_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE);
}

bmcl::Option<msg_t> Sender::sender_poll_msg_data(const raft::Server& from)
{
    return sender_poll_msg_data(from.nodes().get_my_id());
}

bmcl::Option<msg_t> Sender::sender_poll_msg_data(raft::node_id from)
{
    auto& s = _servers[from];
    if (s.outbox.empty())
        return bmcl::None;
    msg_t m = s.outbox.front();
    s.outbox.pop_front();
    return m;
}

bool Sender::sender_msgs_available(raft::node_id from)
{
    auto& s = _servers[from];
    assert(s.raft != nullptr);
    return !s.inbox.empty();
}

void Sender::sender_poll_msgs(raft::node_id from)
{
    auto& s = _servers[from];
    if (!s.raft)
    {
        assert(false);
        return;
    }
    raft::node_id me = s.raft->nodes().get_my_id();

    for(const msg_t& m: s.inbox)
    {
        switch (m.type)
        {
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES:
        {
            EXPECT_EQ(sizeof(msg_appendentries_t), m.data.size());
            auto r = s.raft->accept_appendentries(m.sender, *(msg_appendentries_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_appendentries_response_t response = r.unwrap();
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE:
            EXPECT_EQ(sizeof(msg_appendentries_response_t), m.data.size());
            s.raft->accept_appendentries_response(m.sender, *(msg_appendentries_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE:
        {
            EXPECT_EQ(sizeof(msg_requestvote_t), m.data.size());
            msg_requestvote_response_t response = s.raft->accept_requestvote(from, *(msg_requestvote_t*)m.data.data());
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE:
            EXPECT_EQ(sizeof(msg_requestvote_response_t), m.data.size());
            s.raft->accept_requestvote_response(m.sender, *(msg_requestvote_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_ENTRY:
        {
            EXPECT_EQ(sizeof(msg_entry_t), m.data.size());
            auto r = s.raft->accept_entry(*(msg_entry_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_entry_response_t response = r.unwrap();
            __append_msg(me, m.sender, (msg_entry_t *)&response, sizeof(response), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE);
        }
        break;

        case raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE:
#if 0
            raft_recv_entry_response(me->raft, m->sender, m->data);
#endif
            break;
        }
    }
    s.inbox.clear();
}
