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

using Time = std::chrono::milliseconds;

class Timer
{
    friend class Server;
public:
    Timer(Time ping = Time(200), std::size_t election_factor = 5);
    void set_timeout(Time ping, std::size_t election_factor);

    bool is_time_to_elect() const { return election_timeout_rand <= timeout_elapsed; }
    bool is_time_to_ping() const { return request_timeout <= timeout_elapsed; }
    inline Time get_timeout_elapsed() const { return timeout_elapsed; }
    inline Time get_request_timeout() const { return request_timeout; }
    inline Time get_election_timeout() const { return election_timeout; }
    inline Time get_election_timeout_rand() const { return election_timeout_rand; }
    inline Time get_max_election_timeout() const { return Time(2 * get_election_timeout().count()); }

private:
    inline void add_elapsed(Time elapsed) { timeout_elapsed += elapsed; }
    inline void reset_elapsed() { timeout_elapsed = Time(0); }
    void randomize_election_timeout();

    Time timeout_elapsed;          /**< amount of time left till timeout */
    Time request_timeout;
    Time election_timeout;
    Time election_timeout_rand;
};

}