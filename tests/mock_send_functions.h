#pragma once
#include <deque>
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

typedef struct
{
    std::deque<msg_t> outbox;
    std::deque<msg_t> inbox;
    Raft* raft;
} sender_t;

class Sender
{
public:
    Sender(Raft* raft)
    {
        me.raft = raft;
    }

    void sender_poll_msgs();
    bmcl::Option<msg_t> sender_poll_msg_data();
    bool sender_msgs_available();
    bmcl::Option<RaftError> sender_requestvote(const RaftNode& node, const msg_requestvote_t& msg);
    bmcl::Option<RaftError> sender_requestvote_response(const RaftNode& node, const msg_requestvote_response_t& msg);
    bmcl::Option<RaftError> sender_appendentries(const RaftNode& node, const msg_appendentries_t& msg);
    bmcl::Option<RaftError> sender_appendentries_response(const RaftNode& node, const msg_appendentries_response_t& msg);
    bmcl::Option<RaftError> sender_entries_response(const RaftNode& node, const msg_entry_response_t& msg);

private:
    sender_t me;
};
