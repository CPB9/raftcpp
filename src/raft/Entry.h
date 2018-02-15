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
#include "Ids.h"

namespace raft
{

enum class EntryState : uint8_t
{
    Invalidated,
    NotCommitted,
    Committed,
};

enum class EntryType : uint8_t
{
    User,
    AddNonVotingNode,
    AddNode,
    DemoteNode,
    RemoveNode,
    ChangeCfg
};

inline const char* to_string(EntryType t)
{
    switch (t)
    {
    case EntryType::User: return "User";
    case EntryType::AddNonVotingNode: return "AddNonVotingNode";
    case EntryType::AddNode: return "AddNode";
    case EntryType::DemoteNode: return "DemoteNode";
    case EntryType::RemoveNode: return "RemoveNode";
    case EntryType::ChangeCfg: return "ChangeCfg";
    }
    return "unknown";
}

struct EntryData
{
    EntryData() {}
    EntryData(const std::vector<uint8_t>& data) : data(data) {}
    EntryData(const void* buf, std::size_t len) : data((const uint8_t*)buf, (const uint8_t*)buf + len){ }
    std::vector<uint8_t> data;
};

/** Entry that is stored in the server's entry log. */
struct Entry
{
    Entry(TermId term, EntryId id, EntryData data = EntryData{}) : term(term), id(id), type(EntryType::User), data(data) {}
    Entry(TermId term, EntryId id, EntryType type, NodeId node)
        : term(term), id(id), type(type), node(node) {}
    TermId  term;               /**< the entry's term at the point it was created */
    EntryId id;                 /**< the entry's unique ID */
    EntryType type;             /**< type of entry */
    bmcl::Option<NodeId> node;  /**< node id if this id cfg change entry */
    EntryData data;

    inline bool is_voting_cfg_change() const
    {
        return EntryType::User != type;
    }
};

}
