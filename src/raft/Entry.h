/**
* Copyright (c) 2013, Willem-Hendrik Thiart
* Use of this source code is governed by a BSD-style license that can be
* found in the LICENSE file.
*
* @file
* @brief Representation of a peer
* @author Willem Thiart himself@willemthiart.com
* @version 0.1
*/
#pragma once
#include <vector>
#include <bmcl/Option.h>
#include <bmcl/Either.h>
#include "Ids.h"

namespace raft
{

struct UserData
{
    UserData() {}
    UserData(const std::vector<uint8_t>& data) : data(data) {}
    UserData(const void* buf, std::size_t len) : data((const uint8_t*)buf, (const uint8_t*)buf + len){ }
    std::vector<uint8_t> data;
};

struct InternalData
{
    enum Type
    {
        AddNonVotingNode,
        AddNode,
        DemoteNode,
        RemoveNode,
        Noop,
    };

    InternalData(Type type, NodeId node) : type(type), node(node){}
    Type type;
    NodeId node;

    inline bool is_voting_cfg_change() const { return type == AddNonVotingNode || type == RemoveNode || type == DemoteNode; }
};

inline const char* to_string(InternalData::Type type)
{
    switch (type)
    {
    case InternalData::AddNonVotingNode: return "add nonvoting";
    case InternalData::AddNode: return "add voting";
    case InternalData::DemoteNode: return "demote";
    case InternalData::RemoveNode: return "remove";
    }
    return "unknown";
}

/** Entry that is stored in the server's entry log. */
class Entry
{
private:
    TermId _term;               /**< the entry's term at the point it was created */
    EntryId _id;                 /**< the entry's unique ID */
    bmcl::Either<InternalData, UserData> _data;
public:
    Entry(TermId term, EntryId id, UserData data) : _term(term), _id(id), _data(data) {}
    Entry(TermId term, EntryId id, InternalData data) : _term(term), _id(id), _data(data) {}
    bool isInternal() const { return _data.isFirst(); }
    bool isUser() const { return _data.isSecond(); }
    bmcl::Option<const InternalData&> getInternalData() const { return _data.unwrapFirst(); }
    bmcl::Option<const UserData&> getUserData() const { return _data.unwrapSecond(); }
    TermId  term() const { return _term; }
    EntryId id() const { return _id; }

    static Entry add_node(TermId term, EntryId id, NodeId node) { return Entry(term, id, InternalData(InternalData::AddNode, node)); }
    static Entry remove_node(TermId term, EntryId id, NodeId node) { return Entry(term, id, InternalData(InternalData::RemoveNode, node)); }
    static Entry demote_node(TermId term, EntryId id, NodeId node) { return Entry(term, id, InternalData(InternalData::DemoteNode, node)); }
    static Entry add_nonvoting_node(TermId term, EntryId id, NodeId node) { return Entry(term, id, InternalData(InternalData::AddNonVotingNode, node)); }
    static Entry add_noop(TermId term, EntryId id) { return Entry(term, id, InternalData(InternalData::Noop, NodeId(0))); }
    static Entry user_empty(TermId term, EntryId id) { return Entry(term, id, UserData()); }
};

}
