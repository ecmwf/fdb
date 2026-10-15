// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   May 2019

#pragma once

#include <sys/types.h>
#include <ctime>
#include <utility>

#include "eckit/container/SharedMemArray.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class AvailablePortList {

private:  // types

    // TODO: Cleanup with reaper

    struct Entry {
        int port;
        pid_t pid;
        time_t deadTime;
    };

public:  // methods

    /// Create and populate the port list if it does not exist
    AvailablePortList(int startPort, size_t count);

    /// Obtain unique access to a particular port.
    ///
    /// There is _no_ mechanism for a process to release this access (intentionally).
    /// It is cleaned up by a reaper
    int acquire();

    /// Clean up any processes that have been been dead for deadTime seconds.
    /// Any newly dead processes should be marked with the current time.
    void reap(int deadTime = 60);

    void initialise();

private:  // members

    eckit::SharedMemArray<Entry> shared_;

    int startPort_;
    size_t count_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

