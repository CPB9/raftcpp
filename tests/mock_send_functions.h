#pragma once
#include <deque>
#include <map>
#include <gtest/gtest.h>

enum class raft_message_type_e
{
    RAFT_MSG_REQUESTVOTE,
    RAFT_MSG_REQUESTVOTE_RESPONSE,
    RAFT_MSG_APPENDENTRIES,
    RAFT_MSG_APPENDENTRIES_RESPONSE,
    RAFT_MSG_ENTRY,
    RAFT_MSG_ENTRY_RESPONSE,
};

struct msg_t
{
    msg_t(const void* msg, std::size_t len, raft_message_type_e type, raft_node_id sender)
        : type(type), sender(sender)
    {
        data.assign((uint8_t*)msg, (uint8_t*)msg + len);
    }
    bmcl::Option<msg_requestvote_t*> cast_to_requestvote()
    {
        EXPECT_EQ(raft_message_type_e::RAFT_MSG_REQUESTVOTE, type);
        EXPECT_EQ(data.size(), sizeof(msg_requestvote_t));
        return (msg_requestvote_t*)data.data();
    }
    bmcl::Option<msg_appendentries_t*> cast_to_appendentries()
    {
        EXPECT_EQ(raft_message_type_e::RAFT_MSG_APPENDENTRIES, type);
        EXPECT_EQ(data.size(), sizeof(msg_appendentries_t));
        return (msg_appendentries_t*)data.data();
    }
    std::vector<uint8_t> data;
    /* what type of message is it? */
    raft_message_type_e type;
    /* who sent this? */
    raft_node_id sender;
};


class Sender
{
public:
    explicit Sender(Raft* r = nullptr) { if (r) add(r); }
    void add(Raft* r) { _servers[r->raft_get_my_nodeid()].raft = r; }
    bool sender_msgs_available(raft_node_id from);
    void sender_poll_msgs(raft_node_id from);
    bmcl::Option<msg_t> sender_poll_msg_data(const Raft& from);
    bmcl::Option<msg_t> sender_poll_msg_data(raft_node_id from);
    bmcl::Option<RaftError> sender_requestvote(const Raft* raft, const RaftNode& node, const msg_requestvote_t& msg);
    bmcl::Option<RaftError> sender_requestvote_response(const raft_node_id& from, const raft_node_id& to, const msg_requestvote_response_t& msg);
    bmcl::Option<RaftError> sender_appendentries(const Raft* raft, const RaftNode& node, const msg_appendentries_t& msg);
    bmcl::Option<RaftError> sender_appendentries_response(const raft_node_id& from, const raft_node_id& to, const msg_appendentries_response_t& msg);
    bmcl::Option<RaftError> sender_entries_response(const raft_node_id& from, const raft_node_id& to, const msg_entry_response_t& msg);

private:

    typedef struct
    {
        std::deque<msg_t> outbox;
        std::deque<msg_t> inbox;
        Raft* raft;
    } sender_t;

    bmcl::Option<RaftError> __append_msg(const raft_node_id& from, const raft_node_id& to, const void* data, int len, raft_message_type_e type);

    std::map<raft_node_id, sender_t> _servers;
};
