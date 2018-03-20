#pragma once
#include <vector>

namespace raft
{

enum class NodeId : std::size_t {};
using TermId = std::size_t;
using Index = std::size_t;
using EntryId = std::size_t;
using NodeCount = std::size_t;

struct UserData
{
    UserData() {}
    UserData(const std::vector<uint8_t>& data) : data(data) {}
    UserData(const void* buf, std::size_t len) : data((const uint8_t*)buf, (const uint8_t*)buf + len) { }
    std::vector<uint8_t> data;
};

}
