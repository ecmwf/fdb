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

#ifndef fdb5_api_local_QueryVisitor_H
#define fdb5_api_local_QueryVisitor_H

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Store.h"

#include "eckit/container/Queue.h"

#include "metkit/mars/MarsRequest.h"

namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

template <typename T>
class QueryVisitor : public EntryVisitor {

public: // methods

    using ValueType = T;

    QueryVisitor(eckit::Queue<ValueType>& queue, const metkit::mars::MarsRequest& request) :
        queue_(queue), request_(request) {}

protected: // members

    eckit::Queue<ValueType>& queue_;
    metkit::mars::MarsRequest request_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif
