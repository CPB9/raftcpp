#pragma once
#include <vector>
#include <bmcl/Option.h>
#include "raft/Error.h"
#include "raft/Entry.h"
#include "raft/Ids.h"

namespace raft
{

class DataHandler;

class IIndexAccess
{
public:
    virtual ~IIndexAccess();

    virtual Index count() const = 0;
    virtual bool empty() const = 0;
    virtual bmcl::Option<const Entry&> get_at_idx(Index idx) const = 0;
};

class IStorage : public IIndexAccess
{
public:
    virtual ~IStorage();

    virtual TermId term() const = 0;
    virtual bmcl::Option<NodeId> vote() const = 0;
    virtual bmcl::Option<Error> persist_term_vote(TermId term, bmcl::Option<NodeId> vote) = 0;

    virtual Index get_current_idx() const = 0;
    virtual DataHandler get_from_idx(Index idx) const = 0;
    virtual bmcl::Option<const Entry&> back() const = 0;

    virtual bmcl::Option<Error> push_back(const Entry& c) = 0;
    virtual bmcl::Option<Entry> pop_back() = 0;
};

class DataHandler
{
public:
    explicit DataHandler();
    DataHandler(const Entry* first_entry, Index prev_log_idx, Index count);
    DataHandler(const IIndexAccess* storage, Index prev_log_idx, Index count);
    ~DataHandler();
    Index count() const;
    bool empty() const;
    Index prev_log_idx() const;
    bmcl::Option<const Entry&> get_at_idx(Index idx) const;

private:
    bmcl::Either<const IIndexAccess*, const Entry*> _ptr;
    Index _prev_log_idx;
    Index _count;
};

class MemStorage : public IStorage
{
public:
    MemStorage();

    TermId term() const { return _term; }
    bmcl::Option<NodeId> vote() const { return _vote; }
    bmcl::Option<Error> persist_term_vote(TermId term, bmcl::Option<NodeId> vote) override;

    Index count() const override;
    bool empty() const override;
    Index get_current_idx() const override;
    bmcl::Option<const Entry&> get_at_idx(Index idx) const override;
    DataHandler get_from_idx(Index idx) const override;
    bmcl::Option<const Entry&> back() const override;

    bmcl::Option<Error> push_back(const Entry& c) override;
    bmcl::Option<Entry> pop_back() override;

private:
    TermId _term;
    bmcl::Option<NodeId> _vote;

    Index _base;
    std::vector<Entry> _entries;
};

}