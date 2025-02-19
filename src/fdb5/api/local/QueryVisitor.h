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

#include <tuple>
#include <unordered_map>

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/rules/Rule.h"

#include "eckit/container/Queue.h"

#include "metkit/mars/MarsRequest.h"

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

template <typename T>
class QueryVisitor : public EntryVisitor {

public:  // methods

    using ValueType = T;

    QueryVisitor(eckit::Queue<ValueType>& queue, const metkit::mars::MarsRequest& request) :
        queue_(queue), request_(request) {}

protected:  // methods

    const metkit::mars::MarsRequest& canonicalise(const Rule& rule) const {
        bool success;
        auto it = canonicalised_.find(&rule.registry());
        if (it == canonicalised_.end()) {
            std::tie(it, success) = canonicalised_.emplace(&rule.registry(), rule.registry().canonicalise(request_));
            ASSERT(success);
        }
        return it->second;
    }


protected:  // members

    eckit::Queue<ValueType>& queue_;
    metkit::mars::MarsRequest request_;

private:  // members

    /// Cache of canonicalised requests
    mutable std::unordered_map<const TypesRegistry*, metkit::mars::MarsRequest> canonicalised_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

#endif
