#include "raft/Types.h"

namespace raft
{

ISaver::~ISaver()
{
}

ISender::~ISender()
{
}

const char* to_string(State s)
{
    switch (s)
    {
    case State::Follower: return "follower";
    case State::Candidate: return "candidate";
    case State::Leader: return "leader";
    }
    return "unknown";
}

const char* to_string(ReqVoteState vote)
{
    switch (vote)
    {
    case ReqVoteState::Granted: return "granted";
    case ReqVoteState::NotGranted: return "not granted";
    case ReqVoteState::UnknownNode: return "unknown node";
    }
    return "unknown";
}

const char* to_string(Error e)
{
    switch (e)
    {
    case Error::Shutdown: return "shutdown";
    case Error::NotLeader: return "not a leader";
    case Error::OneVotingChangeOnly: return "the only voting pending voting request allowed";
    case Error::NodeUnknown: return "node unknown";
    case Error::NothingToApply: return "nothing to apply";
    case Error::NothingToSend: return "nothing to send";
    case Error::CantSendToMyself: return "cant send request to myself";
    case Error::NotCandidate: return "not a candidate";
    }
    return "unknown";
}


}
