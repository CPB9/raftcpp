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
    void log_clear();

    /**
    * Add entry to log.
    * Don't add entry if we've already added this entry (based off ID)
    * Don't add entries with ID=0
    * @return 0 if unsucessful; 1 otherwise */
    bmcl::Option<Error> log_append_entry(Server* raft, const raft_entry_t& c);

    /**
    * @return number of entries held within log */
    std::size_t count() const;

    /**
    * Delete all logs from this log onwards */
    void log_delete(Server* raft, std::size_t idx);

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

}