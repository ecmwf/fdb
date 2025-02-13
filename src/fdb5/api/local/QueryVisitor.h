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

#include <unordered_map>

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/rules/Rule.h"

#include "eckit/container/Queue.h"

#include "metkit/mars/MarsRequest.h"

namespace fdb5 {
namespace api {
namespace local {

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
        auto it = canonicalised_.find(&rule);
        if (it == canonicalised_.end()) {
            std::tie(it, success) = canonicalised_.emplace(&rule, rule.registry().canonicalise(request_));
            ASSERT(success);
        }
        return it->second;
    }
        
protected:  // members

    eckit::Queue<ValueType>& queue_;
    metkit::mars::MarsRequest request_;

private:

    struct HashRule {
        std::size_t operator()(const Rule* rule) const {
            return rule->registry().hash();
        }
    };

    struct EqualsRule {
        bool operator()(const Rule* left, const Rule* right) const {
            return left->registry() == right->registry();
        }
    };

private:    // members

    /// Cache of canonicalised requests
    mutable std::unordered_map<const Rule*, metkit::mars::MarsRequest, HashRule, EqualsRule> canonicalised_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
