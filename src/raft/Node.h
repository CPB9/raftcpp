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
#include <bitset>
#include "Types.h"

class RaftNode
{
    enum BitFlags
    {
        RAFT_NODE_VOTED_FOR_ME = 0,
        RAFT_NODE_VOTING       = 1,
        RAFT_NODE_HAS_SUFFICIENT_LOG = 2,
    };

public:
    RaftNode(void* udata, raft_node_id id)
    {
        _me.udata = udata;
        _me.next_idx = 1;
        _me.match_idx = 0;
        _me.id = id;
        _me.flags.set(RAFT_NODE_VOTING, true);
    }

    std::size_t raft_node_get_next_idx() const { return _me.next_idx; }
    void raft_node_set_next_idx(std::size_t nextIdx) {/* log index begins at 1 */ _me.next_idx = nextIdx < 1 ? 1 : nextIdx; }

    std::size_t raft_node_get_match_idx() const { return _me.match_idx; }
    void raft_node_set_match_idx(std::size_t matchIdx) { _me.match_idx = matchIdx; }

    void* raft_node_get_udata() const { return _me.udata; }
    void raft_node_set_udata(void* udata) { _me.udata = udata; }

    bool raft_node_has_vote_for_me() const { return _me.flags.test(RAFT_NODE_VOTED_FOR_ME); }
    void raft_node_vote_for_me(bool vote) { _me.flags.set(RAFT_NODE_VOTED_FOR_ME, vote); }

    void raft_node_set_voting(bool voting) { _me.flags.set(RAFT_NODE_VOTING, voting); }
    bool raft_node_is_voting() const { return _me.flags.test(RAFT_NODE_VOTING); }

    void raft_node_set_has_sufficient_logs() { _me.flags.set(RAFT_NODE_HAS_SUFFICIENT_LOG, true); }
    bool raft_node_has_sufficient_logs() const { return _me.flags.test(RAFT_NODE_HAS_SUFFICIENT_LOG); }

    raft_node_id raft_node_get_id() const { return _me.id; }
    bmcl::Option<raft_node_id> raft_node_get_id_as_option() const { return _me.id; }
private:
    struct raft_node_private_t
    {
        void* udata;
        std::size_t next_idx;
        std::size_t match_idx;
        raft_node_id id;
        std::bitset<8> flags;
    };
    raft_node_private_t _me;
};
