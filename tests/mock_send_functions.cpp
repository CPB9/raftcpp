#include <stdio.h>
#include "raft/Raft.h"
#include "mock_send_functions.h"


bmcl::Option<RaftError> Sender::__append_msg(const raft_node_id& from, const raft_node_id& to, const void* data, int len, raft_message_type_e type)
{
    auto f = _servers[from].raft;
    auto peer = _servers[to].raft;

    assert(_servers[from].raft);

    _servers[from].outbox.emplace_back(data, len, type, f->get_my_nodeid());

    /* give to peer */
    if (peer)
    {
        msg_t m2 = _servers[from].outbox.back();
        m2.sender = from;
        _servers[to].inbox.push_back(m2);
    }

    return bmcl::None;
}

bmcl::Option<RaftError> Sender::sender_requestvote(const Raft * from, const RaftNode & to, const msg_requestvote_t& msg)
{
    return __append_msg(from->get_my_nodeid(), to.get_id(), &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE);
}

bmcl::Option<RaftError> Sender::sender_requestvote_response(const raft_node_id& from, const raft_node_id& to, const msg_requestvote_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
}

bmcl::Option<RaftError> Sender::sender_appendentries(const Raft * from, const RaftNode & to, const msg_appendentries_t& msg)
{
    msg_entry_t* entries = (msg_entry_t*)calloc(1, sizeof(msg_entry_t) * msg.n_entries);
    memcpy(entries, msg.entries, sizeof(msg_entry_t) * msg.n_entries);
    msg_appendentries_t tmp = msg;
    tmp.entries = entries;
    return __append_msg(from->get_my_nodeid(), to.get_id(), &tmp, sizeof(tmp), raft_message_type_e::RAFT_MSG_APPENDENTRIES);
}

bmcl::Option<RaftError> Sender::sender_appendentries_response(const raft_node_id& from, const raft_node_id& to, const msg_appendentries_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
}

bmcl::Option<RaftError> Sender::sender_entries_response(const raft_node_id& from, const raft_node_id& to, const msg_entry_response_t& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE);
}

bmcl::Option<msg_t> Sender::sender_poll_msg_data(const Raft& from)
{
    return sender_poll_msg_data(from.get_my_nodeid());
}

bmcl::Option<msg_t> Sender::sender_poll_msg_data(raft_node_id from)
{
    auto& s = _servers[from];
    if (s.outbox.empty())
        return bmcl::None;
    msg_t m = s.outbox.front();
    s.outbox.pop_front();
    return m;
}

bool Sender::sender_msgs_available(raft_node_id from)
{
    auto& s = _servers[from];
    assert(s.raft != nullptr);
    return !s.inbox.empty();
}

void Sender::sender_poll_msgs(raft_node_id from)
{
    auto& s = _servers[from];
    if (!s.raft)
    {
        assert(false);
        return;
    }
    raft_node_id me = s.raft->get_my_nodeid();

    for(const msg_t& m: s.inbox)
    {
        switch (m.type)
        {
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES:
        {
            EXPECT_EQ(sizeof(msg_appendentries_t), m.data.size());
            auto r = s.raft->raft_recv_appendentries(m.sender, *(msg_appendentries_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_appendentries_response_t response = r.unwrap();
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE:
            EXPECT_EQ(sizeof(msg_appendentries_response_t), m.data.size());
            s.raft->raft_recv_appendentries_response(m.sender, *(msg_appendentries_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE:
        {
            EXPECT_EQ(sizeof(msg_requestvote_t), m.data.size());
            auto r = s.raft->raft_recv_requestvote(m.sender, *(msg_requestvote_t*)m.data.data());
            EXPECT_TRUE(r.isOk());
            msg_requestvote_response_t response = r.unwrap();
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE:
            EXPECT_EQ(sizeof(msg_requestvote_response_t), m.data.size());
            s.raft->raft_recv_requestvote_response(m.sender, *(msg_requestvote_response_t*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_ENTRY:
        {
            EXPECT_EQ(sizeof(msg_entry_t), m.data.size());
            auto r = s.raft->raft_recv_entry(*(msg_entry_t*)m.data.data());
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
