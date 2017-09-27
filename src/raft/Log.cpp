/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "Log.h"
#include "Raft.h"


#define INITIAL_CAPACITY 10


void RaftLog::log_clear()
{
    _me.entries.clear();
    _me.base = 0;
}

std::size_t RaftLog::log_count() const
{
    return _me.entries.size();
}

void RaftLog::log_empty()
{
    _me.entries.clear();
}

std::size_t RaftLog::log_get_current_idx() const
{
    return log_count() + _me.base;
}

bmcl::Option<RaftError> RaftLog::log_append_entry(Raft* raft, const raft_entry_t& c)
{
    if (raft)
    {
        const raft_cbs_t& cb = raft->get_callbacks();
        if (cb.log_offer)
        {
            RaftError e = (RaftError)cb.log_offer(raft, c, log_get_current_idx() + 1);
            raft->raft_offer_log(c, log_get_current_idx() + 1);
            if (e == RaftError::Shutdown)
                return RaftError::Shutdown;
        }
    }

    _me.entries.emplace_back(c);
    return bmcl::None;
}

bmcl::Option<const raft_entry_t*> RaftLog::log_get_from_idx(std::size_t idx, std::size_t *n_etys) const
{
    assert(idx > _me.base);
    /* idx starts at 1 */
    idx -= 1;

    if (idx < _me.base || idx >= _me.base + _me.entries.size())
    {
        *n_etys = 0;
        return bmcl::None;
    }

    std::size_t i = idx - _me.base;
    *n_etys = _me.entries.size() - i;
    return &_me.entries[i];
}

bmcl::Option<const raft_entry_t&> RaftLog::log_get_at_idx(std::size_t idx) const
{
    assert(idx > _me.base);
    /* idx starts at 1 */
    idx -= 1;

    if (idx < _me.base || idx >= _me.base + _me.entries.size())
        return bmcl::None;

    std::size_t i = idx - _me.base;
    return _me.entries[i];

}

void RaftLog::log_delete(Raft* raft, std::size_t idx)
{
    assert(idx > _me.base);

    /* idx starts at 1 */
    idx -= 1;
    idx -= _me.base;
    if (idx >= _me.entries.size())
        return;
    if (idx < 0)
        idx = 0;
    std::size_t count = _me.entries.size() - idx;

    for (std::size_t i = 0; i < count; ++i)
    {
        if (raft && raft->get_callbacks().log_pop)
            raft->get_callbacks().log_pop(raft, _me.entries.back(), _me.base + _me.entries.size());
        raft->raft_pop_log(_me.entries.back(), _me.base + _me.entries.size());
        _me.entries.pop_back();
    }
}

bmcl::Option<raft_entry_t> RaftLog::log_poll(Raft* raft)
{
    if (_me.entries.empty())
        return bmcl::None;

    raft_entry_t elem = _me.entries.front();
    if (raft && raft->get_callbacks().log_poll)
        raft->get_callbacks().log_poll(raft, _me.entries.front(), _me.base + 1);
    _me.entries.erase(_me.entries.begin());
    _me.base++;
    return elem;
}

bmcl::Option<const raft_entry_t&> RaftLog::log_peektail() const
{
    if (_me.entries.empty())
        return bmcl::None;
    return _me.entries.back();
}
