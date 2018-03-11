#pragma once
#include <stdint.h>

namespace raft
{

enum class Error : uint8_t
{
    Shutdown,
    NotFollower,
    NotCandidate,
    NotLeader,
    OneVotingChangeOnly,
    NodeUnknown,
    NothingToApply,
    NothingToSend,
    CantSendToMyself,
    CantSend,
};

const char* to_string(Error e);

}
