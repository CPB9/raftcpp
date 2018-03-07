#include <assert.h>
#include <algorithm>
#include "Committer.h"


namespace raft
{

void Committer::commit_till(Index idx)
{
    if (is_committed(idx))
        return;
    Index last_log_idx = std::max<Index>(get_current_idx(), 1);
    set_commit_idx(std::min(last_log_idx, idx));
}

bmcl::Option<Error> Committer::entry_push_back(const Entry& ety, bool needVoteChecks)
{
    /* Only one voting cfg change at a time */
    bool voting_change = ety.isInternal() && ety.getInternalData()->is_voting_cfg_change();

    if (needVoteChecks && voting_change && voting_change_is_in_progress())
        return Error::OneVotingChangeOnly;

    bmcl::Option<Error> e = _storage->push_back(ety);
    if (e.isSome())
        return e;

    if (voting_change)
        _voting_cfg_change_log_idx = get_current_idx();

    return bmcl::None;
}

bmcl::Result<Entry, Error> Committer::entry_apply_one(ISaver* saver)
{    /* Don't apply after the commit_idx */
    if (!has_not_applied())
        return Error::NothingToApply;

    Index log_idx = _last_applied_idx + 1;

    bmcl::Option<const Entry&> etyo = get_at_idx(log_idx);
    if (etyo.isNone())
        return Error::NothingToApply;
    const Entry& ety = etyo.unwrap();

    _last_applied_idx = log_idx;
    assert(saver);
    bmcl::Option<Error> e = saver->apply_log(ety, _last_applied_idx);
    if (e.isSome())
        return e.unwrap();

    /* voting cfg change is now complete */
    if (log_idx == _voting_cfg_change_log_idx.unwrapOr(0))
        _voting_cfg_change_log_idx.clear();

    return ety;
}

void Committer::set_commit_idx(Index idx)
{
    assert(get_commit_idx() <= idx);
    assert(idx <= get_current_idx());
    _commit_idx = idx;
}

bmcl::Option<TermId> Committer::get_last_log_term() const
{
    const auto& ety = _storage->back();
    if (ety.isNone())
        return bmcl::None;
    return ety->term();
}

bmcl::Option<Entry> Committer::entry_pop_back()
{
    Index idx = get_current_idx();
    if (_storage->empty() || idx <= get_commit_idx())
        return bmcl::None;

    if (idx <= _voting_cfg_change_log_idx.unwrapOr(0))
        _voting_cfg_change_log_idx.clear();

    return _storage->pop_back();
}

EntryState Committer::entry_get_state(const MsgAddEntryRep& r) const
{
    bmcl::Option<const Entry&> ety = get_at_idx(r.idx);
    if (ety.isNone())
        return EntryState::NotCommitted;

    /* entry from another leader has invalidated this entry message */
    if (r.term != ety.unwrap().term())
        return EntryState::Invalidated;
    return is_committed(r.idx) ? EntryState::Committed : EntryState::NotCommitted;
}


}