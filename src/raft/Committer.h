#pragma once
#include <bmcl/Option.h>
#include <bmcl/Result.h>
#include "Types.h"
#include "Storage.h"

namespace raft
{

enum class EntryState : uint8_t
{
    Invalidated,
    NotCommitted,
    Committed,
};

class Committer
{
public:
    explicit Committer(IStorage* storage) : _storage(storage), _commit_idx(0), _last_applied_idx(0) {}
    const IStorage* storage() const { return _storage; }
    inline Index get_commit_idx() const { return _commit_idx; }
    inline Index get_last_applied_idx() const { return _last_applied_idx; }
    inline Index get_current_idx() const { return _storage->get_current_idx(); }
    bmcl::Option<const Entry&> get_at_idx(Index idx) const { return _storage->get_at_idx(idx); }
    bmcl::Option<const Entry*> get_from_idx(Index idx, Index* n_etys) const { return _storage->get_from_idx(idx, n_etys); }

    inline bool has_not_applied() const { return _last_applied_idx < get_commit_idx(); }
    inline bool is_committed(Index idx) const { return idx <= _commit_idx; }
    inline bool is_all_committed() const { return get_last_applied_idx() >= _commit_idx; }
    inline bool voting_change_is_in_progress() const { return _voting_cfg_change_log_idx.isSome(); }
    bmcl::Option<TermId> get_last_log_term() const;
    EntryState entry_get_state(const MsgAddEntryRep& r) const;

    void commit_till(Index idx);
    inline void commit_all() { set_commit_idx(_storage->get_current_idx()); }
    void set_commit_idx(Index idx);

    bmcl::Option<Error> entry_push_back(const Entry& ety, bool needVoteChecks = false);
    bmcl::Result<Entry, Error> entry_apply_one(ISaver* saver);
    bmcl::Option<Entry> entry_pop_back();

private:
    IStorage*   _storage;
    Index       _commit_idx;                           /**< idx of highest log entry known to be committed */
    Index       _last_applied_idx;                     /**< idx of highest log entry applied to state machine */
    bmcl::Option<Index> _voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
};


}