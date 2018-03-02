#include <assert.h>
#include "Storage.h"


namespace raft
{

IStorage::~IStorage() {}

MemStorage::MemStorage(): _base(0), _term(0) { }

Index MemStorage::count() const
{
    return _entries.size();
}

bool MemStorage::empty() const
{
    return _entries.empty();
}

Index MemStorage::get_current_idx() const
{
    return count() + _base;
}

bmcl::Option<Error> MemStorage::push_back(const Entry& c)
{
    _entries.emplace_back(c);
    return bmcl::None;
}

bmcl::Option<const Entry*> MemStorage::get_from_idx(Index idx, Index* n_etys) const
{
    assert(idx > _base);
    /* idx starts at 1 */

    if (idx <= _base || idx > get_current_idx())
    {
        *n_etys = 0;
        return bmcl::None;
    }

    Index i = idx - _base - 1;
    *n_etys = _entries.size() - i;
    return &_entries[i];
}

bmcl::Option<const Entry&> MemStorage::get_at_idx(Index idx) const
{
    assert(idx > _base);
    /* idx starts at 1 */

    if (idx <= _base || idx > get_current_idx())
        return bmcl::None;

    Index i = idx - _base - 1;
    return _entries[i];
}

bmcl::Option<Entry> MemStorage::pop_back()
{
    if (_entries.empty())
        return bmcl::None;
    Entry ety = _entries.back();
    _entries.pop_back();
    return ety;
}

bmcl::Option<const Entry&> MemStorage::back() const
{
    if (_entries.empty())
        return bmcl::None;
    return _entries.back();
}

bmcl::Option<Error> MemStorage::persist_term_vote(TermId term, bmcl::Option<NodeId> vote)
{
    assert(term >= _term);
    assert((term >= _term) || (term == _term && _vote.isNone() && vote.isSome()));
    _term = term;
    _vote = vote;
    return bmcl::None;
}

}