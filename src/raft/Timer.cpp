/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <random>
#include "Timer.h"

namespace raft
{

Timer::Timer()
{
    timeout_elapsed = std::chrono::milliseconds(0);
    set_timeout(std::chrono::milliseconds(200), 5);
    randomize_election_timeout();
}

void Timer::set_timeout(std::chrono::milliseconds msec, std::size_t factor)
{
    request_timeout = msec;
    election_timeout = msec * factor;
    randomize_election_timeout();
}

void Timer::randomize_election_timeout()
{
    /* [election_timeout, 2 * election_timeout) */
    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<std::size_t> distr(election_timeout.count(), 2*election_timeout.count()); // define the range
    election_timeout_rand = std::chrono::milliseconds(distr(eng));
}

}