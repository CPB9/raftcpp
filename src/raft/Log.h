#pragma once
#include <vector>
#include <bmcl/Option.h>
#include "Types.h"

namespace raft
{

class Server;


class Logger
{
    struct log_private_t
    {   /* we compact the log, and thus need to increment the Base Log Index */
        std::size_t base = 0;
        std::vector<raft_entry_t> entries;
    };

public:
    Logger();

    /**
    * Add entry to log.
    * Don't add entry if we've already added this entry (based off ID)
    * Don't add entries with ID=0
    * @return 0 if unsucessful; 1 otherwise */
    void append(const raft_entry_t& c);

    /**
    * @return number of entries held within log */
    std::size_t count() const;

    /**
    * Delete all logs from this log onwards */
    void log_delete_from(Server* raft, std::size_t idx);

    /**
    * Empty the queue. */
    void clear();

    /**
    * Remove oldest entry
    * @return oldest entry */
    bmcl::Option<raft_entry_t> log_poll(Server* raft);

    bmcl::Option<const raft_entry_t*> get_from_idx(std::size_t idx, std::size_t *n_etys) const;

    bmcl::Option<const raft_entry_t&> get_at_idx(std::size_t idx) const;

    /**
    * @return youngest entry */
    bmcl::Option<const raft_entry_t&> peektail() const;

    std::size_t get_current_idx() const;

    bmcl::Option<std::size_t> get_last_log_term() const;

private:
    log_private_t _me;
};

class LogCommitter : public Logger
{
public:
    LogCommitter()
    {
        commit_idx = 0;
        last_applied_idx = 0;
    }

    inline std::size_t get_commit_idx() const { return commit_idx; }
    inline std::size_t get_last_applied_idx() const { return last_applied_idx; }
    inline bool has_not_applied() const { return last_applied_idx < get_commit_idx(); }
    inline bool is_committed(std::size_t idx) const { return idx <= commit_idx; }
    inline bool is_all_committed() const { return get_last_applied_idx() >= commit_idx; }
    inline bool voting_change_is_in_progress() const { return voting_cfg_change_log_idx.isSome(); }

    void commit_till(std::size_t idx);
    inline void commit_all() { set_commit_idx(get_current_idx()); }
    void set_commit_idx(std::size_t idx);

    void entry_delete_from_idx(Server* raft, std::size_t idx);
    bmcl::Option<Error> entry_append(Server* raft, const raft_entry_t& ety);
    bmcl::Option<Error> entry_apply_one(Server* raft);
    bmcl::Option<Error> entry_apply_all(Server* raft);

public:
    inline void set_last_applied_idx(std::size_t idx) { last_applied_idx = idx; }

private:
    std::size_t commit_idx;                                 /**< idx of highest log entry known to be committed */
    std::size_t last_applied_idx;                           /**< idx of highest log entry applied to state machine */
    bmcl::Option<std::size_t> voting_cfg_change_log_idx;    /**< the log which has a voting cfg change */
};


}