#include "raft/Types.h"

namespace raft
{

IEventHandler::~IEventHandler(){}

ISender::~ISender()
{
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

}
