#pragma once
#include <vector>
#include <bmcl/Option.h>
#include "Types.h"

namespace raft
{

class IStorage
{
public:
    virtual ~IStorage();

    virtual TermId term() const = 0;
    virtual bmcl::Option<NodeId> vote() const = 0;
    virtual bmcl::Option<Error> persist_term_vote(TermId term, bmcl::Option<NodeId> vote) = 0;

    virtual Index count() const = 0;
    virtual bool empty() const = 0;
    virtual Index get_current_idx() const = 0;
    virtual bmcl::Option<const Entry&> get_at_idx(Index idx) const = 0;
    virtual bmcl::Option<const Entry*> get_from_idx(Index idx, Index* n_etys) const = 0;
    virtual bmcl::Option<const Entry&> back() const = 0;

    virtual bmcl::Option<Error> push_back(const Entry& c) = 0;
    virtual bmcl::Option<Entry> pop_back() = 0;
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
    bmcl::Option<const Entry*> get_from_idx(Index idx, Index* n_etys) const override;
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