#pragma once
#include <vector>
#include <bmcl/Option.h>
#include <bmcl/Result.h>
#include "Types.h"

namespace raft
{

class Server;


class Logger
{
public:
    Logger();
    Index count() const;
    bool empty() const;

    bmcl::Option<const Entry*> get_from_idx(Index idx, Index* n_etys) const;
    bmcl::Option<const Entry&> get_at_idx(Index idx) const;
    bmcl::Option<const Entry&> back() const;
    bmcl::Option<const Entry&> front() const;
    Index get_current_idx() const;
    Index get_front_idx() const;

protected:
    void append(const Entry& c);
    bmcl::Option<Entry> pop_front();
    bmcl::Option<Entry> pop_back();

private:
    Index _base;
    std::vector<Entry> _entries;
};

class LogCommitter : public Logger
{
public:
    LogCommitter(ISaver* saver) : _saver(saver), _commit_idx(0), _last_applied_idx(0) {}
    inline Index get_commit_idx() const { return _commit_idx; }
    inline Index get_last_applied_idx() const { return _last_applied_idx; }
    inline bool has_not_applied() const { return _last_applied_idx < get_commit_idx(); }
    inline bool is_committed(Index idx) const { return idx <= _commit_idx; }
    inline bool is_all_committed() const { return get_last_applied_idx() >= _commit_idx; }
    inline bool voting_change_is_in_progress() const { return _voting_cfg_change_log_idx.isSome(); }
    bmcl::Option<TermId> get_last_log_term() const;
    EntryState entry_get_state(const MsgAddEntryRep& r) const;

    void commit_till(Index idx);
    inline void commit_all() { set_commit_idx(get_current_idx()); }
    void set_commit_idx(Index idx);

    bmcl::Option<Error> entry_append(const Entry& ety, bool needVoteChecks = false);
    bmcl::Result<Entry, Error> entry_apply_one();
    bmcl::Option<Entry> entry_pop_back();
    void entry_pop_front();

private:
    ISaver* _saver;
    Index _commit_idx;                                 /**< idx of highest log entry known to be committed */
    Index _last_applied_idx;                           /**< idx of highest log entry applied to state machine */
    bmcl::Option<Index> _voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
};


}