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


namespace raft
{

void Logger::log_clear()
{
    _me.entries.clear();
    _me.base = 0;
}

std::size_t Logger::count() const
{
    return _me.entries.size();
}

void Logger::clear()
{
    _me.entries.clear();
}

std::size_t Logger::get_current_idx() const
{
    return count() + _me.base;
}

bmcl::Option<std::size_t> Logger::get_last_log_term() const
{
    std::size_t current_idx = get_current_idx();
    if (0 == current_idx)
        return bmcl::None;

    bmcl::Option<const raft_entry_t&> ety = get_at_idx(current_idx);
    if (ety.isNone())
        return bmcl::None;

    return ety.unwrap().term;
}

void Logger::append(const raft_entry_t& c)
{
    _me.entries.emplace_back(c);
}

bmcl::Option<const raft_entry_t*> Logger::get_from_idx(std::size_t idx, std::size_t *n_etys) const
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

bmcl::Option<const raft_entry_t&> Logger::get_at_idx(std::size_t idx) const
{
    assert(idx > _me.base);
    /* idx starts at 1 */
    idx -= 1;

    if (idx < _me.base || idx >= _me.base + _me.entries.size())
        return bmcl::None;

    std::size_t i = idx - _me.base;
    return _me.entries[i];

}

void Logger::log_delete(Server* raft, std::size_t idx)
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
        raft->pop_log(_me.entries.back(), _me.base + _me.entries.size());
        _me.entries.pop_back();
    }
}

bmcl::Option<raft_entry_t> Logger::log_poll(Server* raft)
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

bmcl::Option<const raft_entry_t&> Logger::peektail() const
{
    if (_me.entries.empty())
        return bmcl::None;
    return _me.entries.back();
}

}