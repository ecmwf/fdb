// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "eckit/container/Queue.h"
#include "eckit/log/Channel.h"
#include "eckit/log/LineBasedTarget.h"

#include <string>

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class QueueStringLogTarget : public eckit::LineBasedTarget {
public:

    QueueStringLogTarget(eckit::Queue<std::string>& queue) : queue_(queue) {}

    void line(const char* line) override { queue_.emplace(std::string(line)); }

private:  // members

    eckit::Queue<std::string>& queue_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

