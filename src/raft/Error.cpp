#include "raft/Error.h"

namespace raft
{

const char* to_string(Error e)
{
    switch (e)
    {
    case Error::Shutdown: return "shutdown";
    case Error::NotFollower: return "not a follower";
    case Error::NotCandidate: return "not a candidate";
    case Error::NotLeader: return "not a leader";
    case Error::OneVotingChangeOnly: return "the only voting pending voting request allowed";
    case Error::NodeUnknown: return "node unknown";
    case Error::NothingToApply: return "nothing to apply";
    case Error::NothingToSend: return "nothing to send";
    case Error::CantSendToMyself: return "cant send request to myself";
    }
    return "unknown";
}

}
