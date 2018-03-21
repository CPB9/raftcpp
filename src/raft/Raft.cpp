/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <assert.h>

#include "raft/Raft.h"

#define RAFT_EVENT() if (_events) _events->

namespace raft
{

const char* to_string(State s)
{
    switch (s)
    {
    case State::Follower: return "follower";
    case State::PreCandidate: return "precandidate";
    case State::Candidate: return "candidate";
    case State::Leader: return "leader";
    case State::Shutdown: return "shutdown";
    }
    return "unknown";
}

Server::Server(NodeId id, bool isNewCluster, const Applier& applyer, IStorage* storage, ISender* sender, IEventHandler* events)
    : _last_cfg_seen(0), _nodes(id), _storage(storage), _committer(storage), _applier(applyer), _sender(sender), _events(&_defaultEventsHandler)
{
    set_event_handler(events);
    _current_term = _storage->term();
    _voted_for = _storage->vote();
    if (isNewCluster)
    {
        _nodes.add_my_node(true);
        become_candidate();
        tick();
        assert(is_leader());
        accept_entry(Entry::add_node(_current_term, 0, id));
    }
    else
    {
        become_follower();
    }
}

Server::Server(NodeId id, bmcl::ArrayView<NodeId> members, const Applier& applyer, IStorage* storage, ISender* sender, IEventHandler* events)
    : _last_cfg_seen(0), _nodes(id), _storage(storage), _committer(storage), _applier(applyer), _sender(sender), _events(&_defaultEventsHandler)
{
    set_event_handler(events);
    _current_term = _storage->term();
    _voted_for = _storage->vote();
    if (members.size() == 1)
    {   /*equivalent to Server(id, isNewCluster=true)*/
        assert(*members.begin() == id);
        _nodes.add_my_node(true);
        become_candidate();
        tick();
        assert(is_leader());
        accept_entry(Entry::add_node(_current_term, 0, id));
    }
    else
    {
        for (const auto& i : members)
        {
            _nodes.add_node(i, true);
        }
        assert(_nodes.get_my_node().isSome());
        become_follower();
    }
}

Server::Server(NodeId id, std::initializer_list<NodeId> members, const Applier& applyer, IStorage* storage, ISender* sender, IEventHandler* events)
    : Server(id, bmcl::ArrayView<NodeId>(members), applyer, storage, sender, events)
{
}

void Server::become_leader()
{
    set_state(State::Leader);
    _timer.reset_elapsed();
    _current_leader = _nodes.get_my_id();

    auto r = accept_entry(Entry::add_noop(_current_term, 0));
    assert(r.isOk());

    for (const Node& i: _nodes.items())
    {
        Node& n = _nodes.get_node(i.get_id()).unwrap();
        n.set_next_idx(_committer.get_current_idx() + 1);
        n.set_match_idx(!n.is_me() ? 0 : _committer.get_current_idx());
        n.set_need_vote_req(false);
        send_appendentries(n, _sender);
    }

    _events->become_leader();
}

void Server::become_candidate()
{
    set_current_term(get_current_term() + 1);
    _nodes.reset_all_votes();
    vote_for_nodeid(_nodes.get_my_id());
    _current_leader.clear();
    set_state(State::Candidate);
    _timer.randomize_election_timeout();
    _timer.reset_elapsed();
    _nodes.set_all_need_pings(false);
    _events->become_candidate();
    _events->radomize_timeouts();

    for (const Node& i : _nodes.items())
    {
        Node& n = _nodes.get_node(i.get_id()).unwrap();
        send_reqvote(n, _sender);
    }
}

void Server::become_precandidate()
{
    _nodes.reset_all_votes();
    set_state(State::PreCandidate);
    _timer.randomize_election_timeout();
    _timer.reset_elapsed();
    _nodes.set_all_need_pings(false);
    _events->become_precandidate();
    _events->radomize_timeouts();

    for (const Node& i : _nodes.items())
    {
        Node& n = _nodes.get_node(i.get_id()).unwrap();
        send_reqvote(n, _sender);
    }
}

void Server::become_follower()
{
    set_state(State::Follower);
    _timer.randomize_election_timeout();
    _timer.reset_elapsed();
    _nodes.set_all_need_vote_req(false);
    _nodes.set_all_need_pings(false);
    _events->become_follower();
    _events->radomize_timeouts();
}

bmcl::Option<Error> Server::tick(Time elapsed_since_last_period, Index max_count)
{
    if (is_shutdown())
        return Error::Shutdown;

    _timer.add_elapsed(elapsed_since_last_period);

    /* Only one voting node means it's safe for us to become the leader */
    if (_nodes.is_me_the_only_voting() && !is_leader())
    {
        vote_for_nodeid(_nodes.get_my_id());
        become_leader();
        if (_nodes.count() == 1)
            _committer.commit_all();
    }

    if (is_leader())
    {
        if (_timer.is_time_to_ping())
        {
            for (const Node& i : _nodes.items())
            {
                send_appendentries(_nodes.get_node(i.get_id()).unwrap(), _sender);
            }
            _timer.reset_elapsed();
        }
    }
    else if (_timer.is_time_to_elect())
    {
        if (_nodes.is_me_candidate_ready())
            become_precandidate();
    }
    return apply_all(max_count);
}

bmcl::Option<Error> Server::apply_all(Index max_count)
{
    Index i = 0;
    while(i < max_count && _committer.has_not_applied())
    {
        bmcl::Option<Error> e = entry_apply_one();
        if (e.isSome())
            return e;
        ++i;
    }
    return bmcl::None;
}

bmcl::Option<Error> Server::accept_rep(NodeId nodeid, const MsgAppendEntriesRep& r)
{
    if (is_shutdown())
        return Error::Shutdown;

    bmcl::Option<Node&> node = _nodes.get_node(nodeid);
    _events->rcvd(nodeid, r);

    if (node.isNone())
        return Error::NodeUnknown;

    if (!is_leader())
        return Error::NotLeader;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (_current_term < r.term)
    {
        bmcl::Option<Error> e = set_current_term(r.term);
        if (e.isSome())
            return e;
        become_follower();
        _current_leader.clear();
        return bmcl::None;
    }

    if (_current_term > r.term)
        return bmcl::None;

    if (!r.success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        Index next_idx = node->get_next_idx();
        assert(0 < next_idx);
        /* Stale response -- ignore */
        assert(node->get_match_idx() <= next_idx - 1);
        if (node->get_match_idx() == next_idx - 1)
            return bmcl::None;

        if (r.current_idx < next_idx - 1)
            node->set_next_idx(std::min<Index>(r.current_idx + 1, _committer.get_current_idx()));
        else
            node->set_next_idx(next_idx - 1);

        /* retry */
        send_appendentries(node.unwrap(), _sender);
        return bmcl::None;
    }

    if (r.current_idx <= node->get_match_idx())
        return bmcl::None;

    assert(r.current_idx <= _committer.get_current_idx());

    node->set_next_idx(r.current_idx + 1);
    node->set_match_idx(r.current_idx);

    if (!node->is_voting() && !_committer.voting_change_is_in_progress() && _committer.get_current_idx() <= r.current_idx + 1)
    {
        auto e = entry_push(Entry::add_node(get_current_term(), EntryId(0), node->get_id()), false);
        if (e.isSome())
            return e;
    }

    /* Update commit idx */
    Index point = r.current_idx;
    if (point > 0)
    {
        bmcl::Option<const Entry&> ety = _committer.get_at_idx(point);
        assert(ety.isSome());
        if (!_committer.is_committed(point) && ety.unwrap().term() == _current_term && _nodes.is_committed(point))
        {
            _committer.set_commit_idx(point);
        }
    }

    /* Aggressively send remaining entries */
    if (_committer.get_at_idx(node->get_next_idx()).isSome())
        send_appendentries(node.unwrap(), _sender);

    /* periodic applies committed entries lazily */

    return bmcl::None;
}

MsgAppendEntriesRep Server::prepare_response(NodeId nodeid, bool success, Index index)
{
    MsgAppendEntriesRep rep(_current_term, success, index);
    _events->send(nodeid, rep);
    return rep;
}

bmcl::Result<MsgAppendEntriesRep, Error> Server::accept_req(NodeId nodeid, const MsgAppendEntriesReq& ae)
{
    if (is_shutdown())
        return Error::Shutdown;

    if (0 < ae.data.count())
        _events->rcvd(nodeid, ae);

    if (_current_term == ae.term)
    {
        assert(!is_leader());
        if (is_candidate() || is_precandidate())
            become_follower();
    }
    else if (ae.term > _current_term)
    {
        set_current_term(ae.term);
        become_follower();
    }
    else if (ae.term < _current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        //__log("AE term %d of %d is less than current term %d", ae.term, nodeid, _current_term);
        return prepare_response(nodeid, false, _committer.get_current_idx());
    }

    /* update current leader because ae->term is up to date */
    _current_leader = nodeid;
    _last_cfg_seen  = ae.last_cfg_seen;
    _timer.reset_elapsed();

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae.data.prev_log_idx())
    {
        bmcl::Option<const Entry&> e = _committer.get_at_idx(ae.data.prev_log_idx());
        if (e.isNone())
        {
            /* 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3) */
            //__log("AE no log at prev_idx %d for ", ae.data.prev_log_idx(), nodeid);
            return prepare_response(nodeid, false, _committer.get_current_idx());
        }
    }

    Index node_current_idx = ae.data.prev_log_idx();

    Index i;
    for (i = 0; i < ae.data.count(); i++)
    {
        Index ety_index = ae.data.prev_log_idx() + 1 + i;
        bmcl::Option<const Entry&> existing_ety = _committer.get_at_idx(ety_index);
        if (existing_ety.isNone())
            break;
        bmcl::Option<const Entry&> ety = ae.data.get_at_idx(ety_index);
        node_current_idx = ety_index;
        if (existing_ety.unwrap().term() != ety->term() && !_committer.is_committed(ety_index))
        {
            /* 3. If an existing entry conflicts with a new one (same index
            but different terms), delete the existing entry and all that
            follow it (§5.3) */
            bool flag = true;
            while (_committer.get_current_idx() >= ety_index)
            {
                bmcl::Option<Entry> pop = _committer.entry_pop_back(/*[this](const InternalData& ety) { pop_log(ety); }*/);
                if (pop.isNone())
                {
                    flag = false;
                    continue;
                }
                entry_pop(pop.unwrap());
                _events->entry_poped(_committer.get_current_idx(), pop.unwrap());
            }
            break;
        }
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < ae.data.count(); i++)
    {
        Index ety_index = ae.data.prev_log_idx() + 1 + i;

        bmcl::Option<const Entry&> ety = ae.data.get_at_idx(ety_index);
        if (ety.isNone())
        {
            //__log(); //unable to deserialize?
            break;
        }

        bmcl::Option<Error> e = entry_push(ety.unwrap(), false);
        if (e.isSome())
        {
            if (e.unwrap() == Error::Shutdown)
            {
                set_state(State::Shutdown);
                //return MsgAppendEntriesRep(current_term, false, node_current_idx, 0);
                return Error::Shutdown;
            }
            break;
        }
        node_current_idx = ety_index;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    _committer.commit_till(ae.leader_commit);
    return prepare_response(nodeid, true, node_current_idx);
}

bool Server::should_grant_vote(const MsgVoteReq& vr) const
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */
    bmcl::Option<const Node&> node = _nodes.get_my_node();
    if (node.isNone() || !node->is_voting())
        return false;

    if (vr.term < get_current_term())
        return false;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    if (!vr.isPre && is_already_voted())
        return false;

    /* Below we check if log is more up-to-date... */

    Index current_idx = _committer.get_current_idx();

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return true;

    bmcl::Option<const Entry&> e = _committer.get_at_idx(current_idx);
    assert((current_idx != 0) == e.isSome());
    if (e.isNone())
        return true;

    if (e.unwrap().term() < vr.last_log_term)
        return true;

    if (vr.last_log_term == e.unwrap().term() && current_idx <= vr.last_log_idx)
        return true;

    return false;
}

MsgVoteRep Server::prepare_requestvote_response_t(NodeId candidate, ReqVoteState vote)
{
    MsgVoteRep rep(get_current_term(), vote);
    _events->send(candidate, rep);
    return rep;
}

bmcl::Result<MsgVoteRep, Error> Server::accept_req(NodeId nodeid, const MsgVoteReq& r)
{
    if (is_shutdown())
        return Error::Shutdown;

    _events->rcvd(nodeid, r);
    if (!r.isPre && get_current_term() < r.term)
    {
        bmcl::Option<Error> e = set_current_term(r.term);
        if (e.isSome())
            return prepare_requestvote_response_t(nodeid, ReqVoteState::NotGranted);
        become_follower();
        _current_leader.clear();
    }

    if (!should_grant_vote(r))
    {
        /* It's possible the candidate node has been removed from the cluster but
        * hasn't received the appendentries that confirms the removal. Therefore
        * the node is partitioned and still thinks its part of the cluster. It
        * will eventually send a requestvote. This is error response tells the
        * node that it might be removed. */
        if (_nodes.get_node(nodeid).isNone())
            return prepare_requestvote_response_t(nodeid, ReqVoteState::UnknownNode);
        return prepare_requestvote_response_t(nodeid, ReqVoteState::NotGranted);
    }

    if (r.isPre)
        return prepare_requestvote_response_t(nodeid, ReqVoteState::Granted);

    /* It shouldn't be possible for a leader or candidate to grant a vote
        * Both states would have voted for themselves */
    assert(is_follower() || is_precandidate());

    _current_leader.clear();
    _timer.reset_elapsed();

    bmcl::Option<Error> e = vote_for_nodeid(nodeid);
    if (e.isSome())
        return prepare_requestvote_response_t(nodeid, ReqVoteState::NotGranted);
    return prepare_requestvote_response_t(nodeid, ReqVoteState::Granted);
}

bmcl::Option<Error> Server::accept_rep(NodeId nodeid, const MsgVoteRep& r)
{
    if (is_shutdown())
        return Error::Shutdown;

    _events->rcvd(nodeid, r);

    if (!is_candidate() && !is_precandidate())
        return bmcl::None;

    if (get_current_term() < r.term)
    {
        set_current_term(r.term);
        become_follower();
        _current_leader.clear();
        return bmcl::None;
    }

    if (get_current_term() > r.term)
    {
        /* The node who voted for us would have obtained our term.
        * Therefore this is an old message we should ignore.
        * This happens if the network is pretty choppy. */
        return bmcl::None;
    }

    switch (r.vote_granted)
    {
    case ReqVoteState::Granted:
    {
        bmcl::Option<Node&> node = _nodes.get_node(nodeid);
        if (node.isSome())
            node->vote_for_me(true);

        if (is_candidate() && _nodes.votes_has_majority(_voted_for))
            become_leader();
        else if (is_precandidate() && _nodes.votes_has_majority(_nodes.get_my_id()))
            become_candidate();
    }
    break;

    case ReqVoteState::NotGranted:
        break;

    case ReqVoteState::UnknownNode:
        set_state(State::Shutdown);
        break;

    default:
        assert(false);
    }


    return bmcl::None;
}

bmcl::Result<MsgAddEntryRep, Error> Server::add_node(EntryId id, NodeId nodeid)
{
    return accept_entry(Entry::add_nonvoting_node(_current_term, id, nodeid));
}

bmcl::Result<MsgAddEntryRep, Error> Server::remove_node(EntryId id, NodeId nodeid)
{
    bmcl::Option<const Node&> node = _nodes.get_node(nodeid);
    if (node.isNone())
        return Error::NodeUnknown;
    return accept_entry(Entry::remove_node(_current_term, id, nodeid));
}

bmcl::Result<MsgAddEntryRep, Error> Server::add_entry(EntryId id, const UserData& data)
{
    return accept_entry(Entry(_current_term, id, data));
}

bmcl::Result<MsgAddEntryRep, Error> Server::accept_entry(const Entry& ety)
{
    if (is_shutdown())
        return Error::Shutdown;

    if (!is_leader())
        return Error::NotLeader;

    _events->entry_rcvd(ety);
    assert(ety.term() == _current_term);
    auto r = entry_push(ety, true);
    if (r.isSome())
        return r.unwrap();

    _events->entry_stored(_committer.get_current_idx() - 1, ety);
    /* if we're the only node, we can consider the entry committed */
    if (_nodes.is_me_the_only_voting())
        _committer.commit_all();

    for (const Node& i: _nodes.items())
    {
        if (i.is_me())
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        Index next_idx = i.get_next_idx();
        if (next_idx == _committer.get_current_idx())
        {
            Node& n = _nodes.get_node(i.get_id()).unwrap();
            send_appendentries(n, _sender);
        }
    }

    return MsgAddEntryRep(_current_term, ety.id(), _committer.get_current_idx());
}

bmcl::Option<Error> Server::entry_apply_one()
{
    if (is_shutdown())
        return Error::Shutdown;

    auto r = _committer.entry_apply_one(_applier);
    if (r.isErr())
    {
        if (r.unwrapErr() == Error::NothingToApply)
            return bmcl::None;
        return r.unwrapErr();
    }

    const Entry& ety = r.unwrap();
    if (r.unwrap().isInternal())
    {
        const InternalData& cmd = ety.getInternalData().unwrap();
        NodeId id = cmd.node;
        bmcl::Option<Node&> node = _nodes.get_node(id);
        switch (cmd.type)
        {
        case InternalData::AddNonVotingNode:
        {
            node = _nodes.add_node(id, false);
            node->set_last_cfg_seen_idx(_committer.get_last_applied_idx());
        }
        break;
        case InternalData::AddNode:
        {
            node = _nodes.add_node(id, true);
            node->set_last_cfg_seen_idx(_committer.get_last_applied_idx());
        }
        break;
        case InternalData::DemoteNode:
        {
            assert(node.isSome());
            if (node.isSome())
                node->set_voting(false);
        }
        break;
        case InternalData::RemoveNode:
        {
            _nodes.remove_node(id);
            if (_nodes.is_me(id) && _last_cfg_seen <= _committer.get_last_applied_idx())
                set_state(State::Shutdown);
        }
        case  InternalData::Noop:
            break;
        default:
            assert(false);
        }
    }

    _events->entry_applied(_committer.get_last_applied_idx() , ety);
    return bmcl::None;
}

void Server::entry_pop(const Entry& ety)
{
    if (ety.isUser())
        return;

    const InternalData& cmd = ety.getInternalData().unwrap();
    NodeId id = cmd.node;
    bmcl::Option<Node&> node;

    switch (cmd.type)
    {
    case InternalData::AddNonVotingNode:
    {
        _nodes.remove_node(id);
    }
    break;
    case InternalData::AddNode:
    {
        node = _nodes.get_node(id);
        if (node.isSome())
            node->set_voting(false);
    }
    break;
    case InternalData::DemoteNode:
    {
        node = _nodes.get_node(id);
        if (node.isSome())
            node->set_voting(true);
    }
    break;
    case InternalData::RemoveNode:
    {
        node = _nodes.add_node(id, false);
    }
    break;
    case InternalData::Noop:
    break;

    default:
        assert(false);
        break;
    }
}

bmcl::Option<Error> Server::entry_push(const Entry& ety, bool needVoteChecks)
{
    auto e = _committer.entry_push_back(ety, needVoteChecks);
    if (e.isSome())
        return e;

    sync_log_and_nodes();

    if (ety.isUser())
        return bmcl::None;

    const InternalData& cmd = ety.getInternalData().unwrap();
    NodeId id = cmd.node;
    bmcl::Option<Node&> node = _nodes.get_node(id);

    switch (cmd.type)
    {
    case InternalData::AddNonVotingNode:
        node = _nodes.add_node(id, false);
        node->set_last_cfg_seen_idx(_committer.get_current_idx());
        break;

    case InternalData::AddNode:
        node = _nodes.add_node(id, true);
        node->set_last_cfg_seen_idx(_committer.get_current_idx());
        break;

    case InternalData::DemoteNode:
        assert(node.isSome());
        if (node.isSome())
            node->set_voting(false);
        break;

    case InternalData::RemoveNode:
        _nodes.remove_node(id);
        break;

    case InternalData::Noop:
        break;

    default:
        assert(false);
    }

    return bmcl::None;
}

bmcl::Option<Error> Server::send_smth_for(NodeId node, ISender* sender)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;

    bmcl::Option<Error> e;
    if (n->need_vote_req())
    {
        e = send_reqvote(n.unwrap(), sender);
        n->set_need_vote_req(false);
        return e;
    }

    if (n->need_append_endtries_req())
    {
        e = send_appendentries(n.unwrap(), sender);
        n->set_need_append_endtries_req(false);
        return e;
    }

    return Error::NothingToSend;
}

bmcl::Option<Error> Server::send_reqvote(Node& node, ISender* sender)
{
    if (_nodes.is_me(node.get_id()))
        return Error::CantSendToMyself;

    if (!is_candidate() && !is_precandidate())
        return Error::NotCandidate;

    if (!sender)
    {
        node.set_need_vote_req(true);
        return bmcl::None;
    }

    if (is_precandidate())
        return sender->request_vote(node.get_id(), MsgVoteReq(_current_term + 1, _committer.get_current_idx(), _committer.get_last_log_term().unwrapOr(TermId(0)), is_precandidate()));

    return sender->request_vote(node.get_id(), MsgVoteReq(_current_term, _committer.get_current_idx(), _committer.get_last_log_term().unwrapOr(TermId(0)), is_precandidate()));
}

bmcl::Option<Error> Server::send_appendentries(NodeId node)
{
    bmcl::Option<Node&> n = _nodes.get_node(node);
    if (n.isNone()) return Error::NodeUnknown;
    return send_appendentries(n.unwrap(), _sender);
}

bmcl::Option<Error> Server::send_appendentries(Node& node, ISender* sender)
{
    if (_nodes.is_me(node.get_id()))
        return Error::CantSendToMyself;

    if (!is_leader())
        return Error::NotLeader;

    if (!sender)
    {
        node.set_need_append_endtries_req(true);
        return bmcl::None;
    }

    Index next_idx = node.get_next_idx();
    MsgAppendEntriesReq ae(_current_term, TermId(0), _committer.get_commit_idx(), node.get_last_cfg_seen_idx(), _committer.get_from_idx(next_idx));

    /* previous log is the log just before the new logs */
    if (1 < next_idx)
    {
        bmcl::Option<const Entry&> prev_ety = _committer.get_at_idx(next_idx - 1);
        if (prev_ety.isSome())
            ae.prev_log_term = prev_ety.unwrap().term();
    }

    _events->send(node.get_id(), ae);
    return sender->append_entries(node.get_id(), ae);
}

bmcl::Option<Error> Server::vote_for_nodeid(NodeId nodeid)
{
    bmcl::Option<Error> e = _storage->persist_term_vote(_current_term, nodeid);
    if (e.isSome())
        return e;
    _voted_for = nodeid;
    return bmcl::None;
}

bmcl::Option<Error> Server::set_current_term(TermId term)
{
    assert(term > _current_term);
    if (_current_term >= term)
        return bmcl::None;

    bmcl::Option<Error> e = _storage->persist_term_vote(term, bmcl::None);
    if (e.isSome())
        return e;

    _current_term = term;
    _voted_for.clear();
    return bmcl::None;
}

void Server::set_state(State state)
{
    /* if became the leader, then update the current leader entry */
    if (state == State::Leader)
        _current_leader = _nodes.get_my_id();
    _state = state;
}

void Server::sync_log_and_nodes()
{
    if (!is_leader())
        return;

    bmcl::Option<Node&> me = _nodes.get_node(_nodes.get_my_id());
    if (me.isNone())
        return;

    me->set_match_idx(_committer.get_current_idx());
    me->set_next_idx(_committer.get_current_idx() + 1);
}

bmcl::Option<Error> Server::start_election()
{
    if (!is_follower())
        return Error::NotFollower;
    become_candidate();
    return bmcl::None;
}

}