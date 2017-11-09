#pragma once
#include <deque>
#include <map>
#include <gtest/gtest.h>

using namespace raft;

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
    msg_t(const void* msg, std::size_t len, raft_message_type_e type, raft::node_id sender)
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
    raft::node_id sender;
};

class Exchanger;

class Sender : public ISender
{
private:
    Exchanger* _ex;
    raft::Server* _r;
public:
    explicit Sender(Exchanger* ex, raft::Server* r);
    bmcl::Option<Error> send_requestvote(const msg_requestvote_t& msg) override;
    bmcl::Option<Error> send_appendentries(const node_id& node, const msg_appendentries_t& msg) override;
};

class Saver : public raft::ISaver
{
    bmcl::Option<Error> applylog(const raft_entry_t& entry, std::size_t entry_idx) override { return bmcl::None; }
    bmcl::Option<Error> persist_vote(node_id node) override { return bmcl::None; }
    bmcl::Option<Error> persist_term(std::size_t node) override { return bmcl::None; }
    bmcl::Option<Error> log_offer(const raft_entry_t& entry, std::size_t entry_idx) override { return bmcl::None; }
    void log_poll(const raft_entry_t& entry, std::size_t entry_idx) override {}
    void log_pop(const raft_entry_t& entry, std::size_t entry_idx) override {}
    void log(const bmcl::Option<const node_id&> node, const char *buf) override {}
};

class Exchanger
{
public:
    explicit Exchanger(raft::Server* r = nullptr) { if (r) add(r); }
    void add(raft::Server* r);
    bool sender_msgs_available(raft::node_id from);
    void sender_poll_msgs(raft::node_id from);
    bmcl::Option<msg_t> sender_poll_msg_data(const raft::Server& from);
    bmcl::Option<msg_t> sender_poll_msg_data(raft::node_id from);
    bmcl::Option<raft::Error> sender_requestvote(const raft::Server* raft, const msg_requestvote_t& msg);
    bmcl::Option<raft::Error> sender_requestvote_response(const raft::node_id& from, const raft::node_id& to, const msg_requestvote_response_t& msg);
    bmcl::Option<raft::Error> sender_appendentries(const raft::Server* raft, const raft::node_id& node, const msg_appendentries_t& msg);
    bmcl::Option<raft::Error> sender_appendentries_response(const raft::node_id& from, const raft::node_id& to, const msg_appendentries_response_t& msg);
    bmcl::Option<raft::Error> sender_entries_response(const raft::node_id& from, const raft::node_id& to, const msg_entry_response_t& msg);

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

    bmcl::Option<raft::Error> __append_msg(const raft::node_id& from, const raft::node_id& to, const void* data, int len, raft_message_type_e type);

    std::map<raft::node_id, sender_t> _servers;
};
