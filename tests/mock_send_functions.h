#pragma once
#include <deque>
#include <map>
#include <gtest/gtest.h>
#include "raft/Raft.h"

using namespace raft;

enum class raft_message_type_e
{
    RAFT_MSG_REQUESTVOTE,
    RAFT_MSG_REQUESTVOTE_RESPONSE,
    RAFT_MSG_APPENDENTRIES,
    RAFT_MSG_APPENDENTRIES_RESPONSE,
};

struct msg_t
{
    msg_t(const void* msg, std::size_t len, raft_message_type_e type, raft::NodeId sender)
        : type(type), sender(sender)
    {
        data.assign((uint8_t*)msg, (uint8_t*)msg + len);
    }
    bmcl::Option<MsgVoteReq*> cast_to_requestvote()
    {
        EXPECT_EQ(raft_message_type_e::RAFT_MSG_REQUESTVOTE, type);
        EXPECT_EQ(data.size(), sizeof(MsgVoteReq));
        return (MsgVoteReq*)data.data();
    }
    bmcl::Option<MsgAppendEntriesReq*> cast_to_appendentries()
    {
        EXPECT_EQ(raft_message_type_e::RAFT_MSG_APPENDENTRIES, type);
        EXPECT_EQ(data.size(), sizeof(MsgAppendEntriesReq));
        return (MsgAppendEntriesReq*)data.data();
    }
    std::vector<uint8_t> data;
    /* what type of message is it? */
    raft_message_type_e type;
    /* who sent this? */
    raft::NodeId sender;
};

class Exchanger;

class Sender : public ISender
{
private:
    Exchanger* _ex;
    raft::Server* _r;
public:
    explicit Sender(Exchanger* ex, raft::Server* r);
    bmcl::Option<Error> request_vote(const NodeId& node, const MsgVoteReq& msg) override;
    bmcl::Option<Error> append_entries(const NodeId& node, const MsgAppendEntriesReq& msg) override;
};

class Saver : public raft::ISaver
{
    bmcl::Option<Error> apply_log(const Entry& entry, std::size_t entry_idx) override { return bmcl::None; }
    void log(const char *buf) override {}
};

class Exchanger
{
public:
    explicit Exchanger(raft::Server* r = nullptr) { if (r) add(r); }
    void add(raft::Server* r);
    void clear();
    bool msgs_available(raft::NodeId from);
    void poll_msgs(raft::NodeId from);
    bmcl::Option<msg_t> poll_msg_data(const raft::Server& from);
    bmcl::Option<msg_t> poll_msg_data(raft::NodeId from);
    bmcl::Option<raft::Error> request_vote_req(const raft::Server* raft, const raft::NodeId& node, const MsgVoteReq& msg);
    bmcl::Option<raft::Error> request_vote_rep(const raft::NodeId& from, const raft::NodeId& to, const MsgVoteRep& msg);
    bmcl::Option<raft::Error> append_entries_req(const raft::Server* raft, const raft::NodeId& node, const MsgAppendEntriesReq& msg);
    bmcl::Option<raft::Error> append_entries_rep(const raft::NodeId& from, const raft::NodeId& to, const MsgAppendEntriesRep& msg);

private:

    struct sender_t
    {
        sender_t() = default;
        sender_t(Exchanger* ex, raft::Server* r)
        {
            raft = r;
            sender = std::make_shared<Sender>(ex, r);
        }
        std::deque<msg_t> outbox;
        std::deque<msg_t> inbox;
        raft::Server* raft;
        std::shared_ptr<ISender> sender;
    };

    bmcl::Option<raft::Error> __append_msg(const raft::NodeId& from, const raft::NodeId& to, const void* data, int len, raft_message_type_e type);

    std::map<raft::NodeId, sender_t> _servers;
};
