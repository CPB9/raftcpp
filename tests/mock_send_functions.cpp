#include <stdio.h>
#include "raft/Raft.h"
#include "mock_send_functions.h"

Sender::Sender(Exchanger* ex, raft::Server* r) : _ex(ex), _r(r) {}
bmcl::Option<Error> Sender::request_vote(const NodeId& node, const MsgVoteReq& msg)
{
    return _ex->request_vote_req(_r, node, msg);
}

bmcl::Option<Error> Sender::append_entries(const NodeId& node, const MsgAppendEntriesReq& msg) 
{
    return _ex->append_entries_req(_r, node, msg);
}

void Exchanger::add(raft::Server* r)
{
    sender_t s(this, r);
    _servers.emplace(r->nodes().get_my_id(), s);
    r->set_sender(s.sender.get());
}

void Exchanger::clear()
{
    for (auto& i : _servers)
    {
        i.second.inbox.clear();
        i.second.outbox.clear();
    }
}

bmcl::Option<raft::Error> Exchanger::__append_msg(const raft::NodeId& from, const raft::NodeId& to, const void* data, int len, raft_message_type_e type)
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

bmcl::Option<raft::Error> Exchanger::request_vote_req(const raft::Server* raft, const raft::NodeId& node, const MsgVoteReq& msg)
{
    //return __append_msg(from->nodes().get_my_id(), to.get_id(), &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE);
    for (const auto& to : _servers)
    {
        if (to.first != raft->nodes().get_my_id())
            __append_msg(raft->nodes().get_my_id(), node, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE);
    }
    return bmcl::None;
}

bmcl::Option<raft::Error> Exchanger::request_vote_rep(const raft::NodeId& from, const raft::NodeId& to, const MsgVoteRep& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
}

bmcl::Option<raft::Error> Exchanger::append_entries_req(const raft::Server * from, const raft::NodeId & to, const MsgAppendEntriesReq& msg)
{
    MsgAddEntryReq* entries = (MsgAddEntryReq*)calloc(1, sizeof(MsgAddEntryReq) * msg.n_entries);
    memcpy(entries, msg.entries, sizeof(MsgAddEntryReq) * msg.n_entries);
    MsgAppendEntriesReq tmp = msg;
    tmp.entries = entries;
    return __append_msg(from->nodes().get_my_id(), to, &tmp, sizeof(tmp), raft_message_type_e::RAFT_MSG_APPENDENTRIES);
}

bmcl::Option<raft::Error> Exchanger::append_entries_rep(const raft::NodeId& from, const raft::NodeId& to, const MsgAppendEntriesRep& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
}

bmcl::Option<raft::Error> Exchanger::entries_rep(const raft::NodeId& from, const raft::NodeId& to, const MsgAddEntryRep& msg)
{
    return __append_msg(from, to, &msg, sizeof(msg), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE);
}

bmcl::Option<msg_t> Exchanger::poll_msg_data(const raft::Server& from)
{
    return poll_msg_data(from.nodes().get_my_id());
}

bmcl::Option<msg_t> Exchanger::poll_msg_data(raft::NodeId from)
{
    auto& s = _servers[from];
    if (s.outbox.empty())
        return bmcl::None;
    msg_t m = s.outbox.front();
    s.outbox.pop_front();
    return m;
}

bool Exchanger::msgs_available(raft::NodeId from)
{
    auto& s = _servers[from];
    assert(s.raft != nullptr);
    return !s.inbox.empty();
}

void Exchanger::poll_msgs(raft::NodeId from)
{
    auto& s = _servers[from];
    if (!s.raft)
    {
        assert(false);
        return;
    }
    raft::NodeId me = s.raft->nodes().get_my_id();

    for(const msg_t& m: s.inbox)
    {
        switch (m.type)
        {
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES:
        {
            EXPECT_EQ(sizeof(MsgAppendEntriesReq), m.data.size());
            auto r = s.raft->accept_req(m.sender, *(MsgAppendEntriesReq*)m.data.data());
            EXPECT_TRUE(r.isOk());
            MsgAppendEntriesRep response = r.unwrap();
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_APPENDENTRIES_RESPONSE:
            EXPECT_EQ(sizeof(MsgAppendEntriesRep), m.data.size());
            s.raft->accept_rep(m.sender, *(MsgAppendEntriesRep*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE:
        {
            EXPECT_EQ(sizeof(MsgVoteReq), m.data.size());
            MsgVoteRep response = s.raft->accept_req(m.sender, *(MsgVoteReq*)m.data.data());
            __append_msg(me, m.sender, &response, sizeof(response), raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE);
        }
        break;
        case raft_message_type_e::RAFT_MSG_REQUESTVOTE_RESPONSE:
            EXPECT_EQ(sizeof(MsgVoteRep), m.data.size());
            s.raft->accept_rep(m.sender, *(MsgVoteRep*)m.data.data());
            break;
        case raft_message_type_e::RAFT_MSG_ENTRY:
        {
            EXPECT_EQ(sizeof(MsgAddEntryReq), m.data.size());
            auto r = s.raft->accept_entry(*(MsgAddEntryReq*)m.data.data());
            EXPECT_TRUE(r.isOk());
            MsgAddEntryRep response = r.unwrap();
            __append_msg(me, m.sender, (MsgAddEntryReq *)&response, sizeof(response), raft_message_type_e::RAFT_MSG_ENTRY_RESPONSE);
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
