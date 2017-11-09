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
    std::size_t count() const;
    bool empty() const;

    bmcl::Option<const raft_entry_t*> get_from_idx(std::size_t idx, std::size_t *n_etys) const;
    bmcl::Option<const raft_entry_t&> get_at_idx(std::size_t idx) const;
    bmcl::Option<const raft_entry_t&> back() const;
    bmcl::Option<const raft_entry_t&> front() const;
    std::size_t get_current_idx() const;
    std::size_t get_front_idx() const;

protected:
    void append(const raft_entry_t& c);
    bmcl::Option<raft_entry_t> pop_front();
    bmcl::Option<raft_entry_t> pop_back();

private:
    std::size_t _base;
    std::vector<raft_entry_t> _entries;
};

class LogCommitter : public Logger
{
public:
    LogCommitter(ISaver* saver) : _saver(saver)
    {
        _commit_idx = 0;
        _last_applied_idx = 0;
    }

    inline std::size_t get_commit_idx() const { return _commit_idx; }
    inline std::size_t get_last_applied_idx() const { return _last_applied_idx; }
    inline bool has_not_applied() const { return _last_applied_idx < get_commit_idx(); }
    inline bool is_committed(std::size_t idx) const { return idx <= _commit_idx; }
    inline bool is_all_committed() const { return get_last_applied_idx() >= _commit_idx; }
    inline bool voting_change_is_in_progress() const { return _voting_cfg_change_log_idx.isSome(); }
    bmcl::Option<std::size_t> get_last_log_term() const;

    void commit_till(std::size_t idx);
    inline void commit_all() { set_commit_idx(get_current_idx()); }
    void set_commit_idx(std::size_t idx);

    bmcl::Option<Error> entry_append(const raft_entry_t& ety);
    bmcl::Result<raft_entry_t, Error> entry_apply_one();
    bmcl::Option<raft_entry_t> entry_pop_back();
    void entry_pop_front();

private:
    ISaver* _saver;
    std::size_t _commit_idx;                                 /**< idx of highest log entry known to be committed */
    std::size_t _last_applied_idx;                           /**< idx of highest log entry applied to state machine */
    bmcl::Option<std::size_t> _voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
};


}