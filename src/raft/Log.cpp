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
#include <algorithm>

#include "Log.h"
#include "Raft.h"


namespace raft
{

Logger::Logger()
{
    _me.base = 0;
}

std::size_t Logger::count() const
{
    return _me.entries.size();
}

bool Logger::empty() const
{
    return _me.entries.empty();
}

std::size_t Logger::get_current_idx() const
{
    return count() + _me.base;
}

std::size_t Logger::get_front_idx() const
{
    return _me.base + 1;
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

bmcl::Option<raft_entry_t> Logger::pop_back()
{
    if (_me.entries.empty())
        return bmcl::None;
    raft_entry_t ety = _me.entries.back();
    _me.entries.pop_back();
    return ety;
}

bmcl::Option<raft_entry_t> Logger::pop_front()
{
    if (_me.entries.empty())
        return bmcl::None;
    raft_entry_t elem = _me.entries.front();
    _me.entries.erase(_me.entries.begin());
    _me.base++;
    return elem;
}

bmcl::Option<const raft_entry_t&> Logger::back() const
{
    if (_me.entries.empty())
        return bmcl::None;
    return _me.entries.back();
}

bmcl::Option<const raft_entry_t&> Logger::front() const
{
    if (_me.entries.empty())
        return bmcl::None;
    return _me.entries.front();
}

void LogCommitter::log_delete_from(Server* raft, std::size_t idx)
{
    for (std::size_t i = get_current_idx(); i >= idx && !empty(); --i)
    {
        auto ety = pop_back();
        if (ety.isNone())
            return;
        if (raft && raft->get_callbacks().log_pop)
            raft->get_callbacks().log_pop(raft, ety.unwrap(), i);
        raft->pop_log(ety.unwrap(), i);

    }
}

void LogCommitter::commit_till(std::size_t idx)
{
    if (is_committed(idx))
        return;
    std::size_t last_log_idx = std::max<std::size_t>(get_current_idx(), 1);
    set_commit_idx(std::min(last_log_idx, idx));
}

void LogCommitter::entry_delete_from_idx(Server* raft, std::size_t idx)
{
    assert(!is_committed(idx));
    if (idx <= voting_cfg_change_log_idx.unwrapOr(0))
        voting_cfg_change_log_idx.clear();
    log_delete_from(raft, idx);
}

bmcl::Option<Error> LogCommitter::entry_append(Server* raft, const raft_entry_t& ety)
{
    /* Only one voting cfg change at a time */
    if (ety.is_voting_cfg_change() && voting_change_is_in_progress())
        return Error::OneVotingChangeOnly;

    if (ety.is_voting_cfg_change())
        voting_cfg_change_log_idx = get_current_idx();

    if (raft && raft->get_callbacks().log_offer)
    {
        Error e = (Error)raft->get_callbacks().log_offer(raft, ety, get_current_idx() + 1);
        if (e == Error::Shutdown)
            return Error::Shutdown;
    }

    append(ety);
    return bmcl::None;
}

bmcl::Option<Error> LogCommitter::entry_apply_one(Server* raft)
{    /* Don't apply after the commit_idx */
    if (!has_not_applied())
        return bmcl::None;

    std::size_t log_idx = last_applied_idx + 1;

    bmcl::Option<const raft_entry_t&> ety = get_at_idx(log_idx);
    if (ety.isNone())
        return bmcl::None;

    //__log(NULL, "applying log: %d, id: %d size: %d", last_applied_idx, ety.unwrap().id, ety.unwrap().data.len);

    last_applied_idx = log_idx;
    assert(raft && raft->get_callbacks().applylog);
    int e = raft->get_callbacks().applylog(raft, ety.unwrap(), last_applied_idx - 1);
    if (e == RAFT_ERR_SHUTDOWN)
        return Error::Shutdown;
    assert(e == 0);

    /* Membership Change: confirm connection with cluster */
    if (logtype_e::ADD_NODE == ety.unwrap().type)
    {
        node_id id = (node_id)raft->get_callbacks().log_get_node_id(raft, ety.unwrap(), log_idx);
        raft->entry_apply_node_add(ety.unwrap(), id);
    }

    /* voting cfg change is now complete */
    if (log_idx == voting_cfg_change_log_idx.unwrapOr(0))
        voting_cfg_change_log_idx.clear();

    return bmcl::None;
}

bmcl::Option<Error> LogCommitter::entry_apply_all(Server* raft)
{
    while (is_all_committed())
    {
        bmcl::Option<Error> e = entry_apply_one(raft);
        if (e.isSome())
            return e;
    }

    return bmcl::None;
}

void LogCommitter::set_commit_idx(std::size_t idx)
{
    assert(get_commit_idx() <= idx);
    assert(idx <= get_current_idx());
    commit_idx = idx;
}

bmcl::Option<std::size_t> LogCommitter::get_last_log_term() const
{
    const auto& ety = back();
    if (ety.isNone())
        return bmcl::None;
    return ety->term;
}

void LogCommitter::entry_pop_back(Server* raft)
{
    entry_delete_from_idx(raft, get_current_idx());
}

void LogCommitter::entry_pop_front(Server* raft)
{
    auto ety = pop_front();
    if (ety.isNone())
        return;
    if (raft && raft->get_callbacks().log_poll)
        raft->get_callbacks().log_poll(raft, ety.unwrap(), get_front_idx());
    pop_front();
}

}