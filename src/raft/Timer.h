/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#pragma once
#include <chrono>

namespace raft
{

class Timer
{
public:
    Timer();
    inline void add_elapsed(std::chrono::milliseconds msec) { timeout_elapsed += msec; }
    inline void reset_elapsed() { timeout_elapsed = std::chrono::milliseconds(0); }
    void set_timeout(std::chrono::milliseconds msec, std::size_t factor);
    void randomize_election_timeout();

    bool is_time_to_elect() const { return election_timeout_rand <= timeout_elapsed; }
    bool is_time_to_ping() const { return request_timeout <= timeout_elapsed; }
    inline std::chrono::milliseconds get_timeout_elapsed() const { return timeout_elapsed; }
    inline std::chrono::milliseconds get_request_timeout() const { return request_timeout; }
    inline std::chrono::milliseconds get_election_timeout() const { return election_timeout; }
    inline std::chrono::milliseconds get_election_timeout_rand() const { return election_timeout_rand; }
    inline std::chrono::milliseconds get_max_election_timeout() const { return std::chrono::milliseconds(2 * get_election_timeout().count()); }
private:
    std::chrono::milliseconds timeout_elapsed;          /**< amount of time left till timeout */
    std::chrono::milliseconds request_timeout;
    std::chrono::milliseconds election_timeout;
    std::chrono::milliseconds election_timeout_rand;
};

}