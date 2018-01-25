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


namespace raft
{

Logger::Logger(): _base(0) { }

Index Logger::count() const
{
    return _entries.size();
}

bool Logger::empty() const
{
    return _entries.empty();
}

Index Logger::get_current_idx() const
{
    return count() + _base;
}

Index Logger::get_front_idx() const
{
    return _base + 1;
}

void Logger::append(const LogEntry& c)
{
    _entries.emplace_back(c);
}

bmcl::Option<const LogEntry*> Logger::get_from_idx(Index idx, Index* n_etys) const
{
    assert(idx > _base);
    /* idx starts at 1 */
    idx -= 1;

    if (idx < _base || idx >= _base + _entries.size())
    {
        *n_etys = 0;
        return bmcl::None;
    }

    Index i = idx - _base;
    *n_etys = _entries.size() - i;
    return &_entries[i];
}

bmcl::Option<const LogEntry&> Logger::get_at_idx(Index idx) const
{
    assert(idx > _base);
    /* idx starts at 1 */
    idx -= 1;

    if (idx < _base || idx >= _base + _entries.size())
        return bmcl::None;

    Index i = idx - _base;
    return _entries[i];
}

bmcl::Option<LogEntry> Logger::pop_back()
{
    if (_entries.empty())
        return bmcl::None;
    LogEntry ety = _entries.back();
    _entries.pop_back();
    return ety;
}

bmcl::Option<LogEntry> Logger::pop_front()
{
    if (_entries.empty())
        return bmcl::None;
    LogEntry elem = _entries.front();
    _entries.erase(_entries.begin());
    _base++;
    return elem;
}

bmcl::Option<const LogEntry&> Logger::back() const
{
    if (_entries.empty())
        return bmcl::None;
    return _entries.back();
}

bmcl::Option<const LogEntry&> Logger::front() const
{
    if (_entries.empty())
        return bmcl::None;
    return _entries.front();
}

void LogCommitter::commit_till(Index idx)
{
    if (is_committed(idx))
        return;
    Index last_log_idx = std::max<Index>(get_current_idx(), 1);
    set_commit_idx(std::min(last_log_idx, idx));
}

bmcl::Option<Error> LogCommitter::entry_append(const LogEntry& ety, bool needVoteChecks)
{
    /* Only one voting cfg change at a time */
    if (_saver)
    {
        bmcl::Option<Error> e = _saver->push_back(ety, get_current_idx() + 1);
        if (e.isSome())
            return e;
    }

    append(ety);
    if (ety.is_cfg_change())
        _voting_cfg_change_log_idx = get_current_idx();

    return bmcl::None;
}

bmcl::Result<LogEntry, Error> LogCommitter::entry_apply_one()
{    /* Don't apply after the commit_idx */
    if (!has_not_applied())
        return Error::NothingToApply;

    Index log_idx = _last_applied_idx + 1;

    bmcl::Option<const LogEntry&> etyo = get_at_idx(log_idx);
    if (etyo.isNone())
        return Error::NothingToApply;
    const LogEntry& ety = etyo.unwrap();

    _last_applied_idx = log_idx;
    assert(_saver);
    bmcl::Option<Error> e = _saver->apply_log(ety, _last_applied_idx - 1);
    if (e.isSome())
        return e.unwrap();

    /* voting cfg change is now complete */
//  if (log_idx == _voting_cfg_change_log_idx.unwrapOr(0))
//      _voting_cfg_change_log_idx.clear();

    return ety;
}

void LogCommitter::set_commit_idx(Index idx)
{
    assert(get_commit_idx() <= idx);
    assert(idx <= get_current_idx());
    _commit_idx = idx;
}

bmcl::Option<TermId> LogCommitter::get_last_log_term() const
{
    const auto& ety = back();
    if (ety.isNone())
        return bmcl::None;
    return ety->term;
}

bmcl::Option<LogEntry> LogCommitter::entry_pop_back()
{
    Index idx = get_current_idx();
    if (empty() || idx <= get_commit_idx())
        return bmcl::None;

    if (idx <= _voting_cfg_change_log_idx.unwrapOr(0))
        _voting_cfg_change_log_idx.clear();

    auto ety = pop_back();
    if (ety.isNone())
        return bmcl::None;
    _saver->pop_back(ety.unwrap(), idx);
    return ety;
}

void LogCommitter::entry_pop_front()
{
    auto ety = pop_front();
    if (ety.isNone())
        return;
    if (_saver)
        _saver->pop_front(ety.unwrap(), get_front_idx());
    pop_front();
}

EntryState LogCommitter::entry_get_state(const MsgAddEntryRep& r) const
{
    bmcl::Option<const LogEntry&> ety = get_at_idx(r.idx);
    if (ety.isNone())
        return EntryState::NotCommitted;

    /* entry from another leader has invalidated this entry message */
    if (r.term != ety.unwrap().term)
        return EntryState::Invalidated;
    return is_committed(r.idx) ? EntryState::Committed : EntryState::NotCommitted;
}


}