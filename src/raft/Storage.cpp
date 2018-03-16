#include <assert.h>
#include "raft/Storage.h"


namespace raft
{

IIndexAccess::~IIndexAccess() {}
IStorage::~IStorage() {}

DataHandler::DataHandler() : _ptr((const Entry*)nullptr), _prev_log_idx(0), _count(0){}
DataHandler::DataHandler(const Entry* first_entry, Index prev_log_idx, Index count) : _ptr(first_entry), _prev_log_idx(prev_log_idx), _count(count) {}
DataHandler::DataHandler(const IIndexAccess* storage, Index prev_log_idx, Index count) : _ptr(storage), _prev_log_idx(prev_log_idx), _count(count)
{
    assert(_count <= storage->count() - _prev_log_idx);
}

DataHandler::~DataHandler() {}
Index DataHandler::count() const { return _count; }
bool DataHandler::empty() const { return _count == 0; }
Index DataHandler::prev_log_idx() const { return _prev_log_idx; }

bmcl::Option<const Entry&> DataHandler::get_at_idx(Index idx) const
{
    if (idx <= _prev_log_idx || idx > _prev_log_idx + _count)
        return bmcl::None;

    if (_ptr.isFirst())
        return _ptr.unwrapFirst()->get_at_idx(idx);
    else
        return _ptr.unwrapSecond()[idx - _prev_log_idx - 1];
}


MemStorage::MemStorage(): _base(0), _term(0) { }

Index MemStorage::count() const
{
    return (Index)_entries.size();
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

DataHandler MemStorage::get_from_idx(Index idx) const
{
    assert(idx > _base);
    /* idx starts at 1 */

    if (idx <= _base || idx > get_current_idx())
        return DataHandler((IStorage*)this, idx - 1, Index(0));

    Index i = idx - _base - 1;
    return DataHandler((IStorage*)this, idx - 1, Index( _entries.size() - i));
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