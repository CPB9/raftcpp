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

namespace raft
{

class Node
{
    enum BitFlags
    {
        RAFT_NODE_VOTED_FOR_ME = 0,
        RAFT_NODE_VOTING       = 1,
        RAFT_NODE_HAS_SUFFICIENT_LOG = 2,
    };

public:
    explicit Node(node_id id)
    {
        _me.next_idx = 1;
        _me.match_idx = 0;
        _me.id = id;
        _me.flags.set(RAFT_NODE_VOTING, true);
    }

    node_id get_id() const { return _me.id; }

    std::size_t get_next_idx() const { return _me.next_idx; }
    void set_next_idx(std::size_t nextIdx) {/* log index begins at 1 */ _me.next_idx = nextIdx < 1 ? 1 : nextIdx; }

    std::size_t get_match_idx() const { return _me.match_idx; }
    void set_match_idx(std::size_t matchIdx) { _me.match_idx = matchIdx; }

    bool has_vote_for_me() const { return _me.flags.test(RAFT_NODE_VOTED_FOR_ME); }
    void vote_for_me(bool vote) { _me.flags.set(RAFT_NODE_VOTED_FOR_ME, vote); }

    void set_voting(bool voting) { _me.flags.set(RAFT_NODE_VOTING, voting); }
    bool is_voting() const { return _me.flags.test(RAFT_NODE_VOTING); }

    void set_has_sufficient_logs() { _me.flags.set(RAFT_NODE_HAS_SUFFICIENT_LOG, true); }
    bool has_sufficient_logs() const { return _me.flags.test(RAFT_NODE_HAS_SUFFICIENT_LOG); }

private:
    struct node_private_t
    {
        std::size_t next_idx;
        std::size_t match_idx;
        node_id id;
        std::bitset<8> flags;
    };
    node_private_t _me;
};

}