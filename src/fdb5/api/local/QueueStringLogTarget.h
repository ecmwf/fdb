/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_local_QueueStringLogTarget_H
#define fdb5_api_local_QueueStringLogTarget_H

#include "eckit/container/Queue.h"
#include "eckit/log/Channel.h"
#include "eckit/log/LineBasedTarget.h"

#include <string>

namespace fdb5 {
namespace api {
namespace local {

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

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
