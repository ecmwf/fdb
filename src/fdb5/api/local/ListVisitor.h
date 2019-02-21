/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_local_ListVisitor_H
#define fdb5_api_local_ListVisitor_H

#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/ListIterator.h"

namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct ListVisitor : public QueryVisitor<ListElement> {

public:
    using QueryVisitor::QueryVisitor;

    bool visitDatabase(const DB& db) {
        bool ret = QueryVisitor::visitDatabase(db);

        // Subselect the parts of the request
        indexRequest_ = request_;
        for (const auto& kv : db.key()) {
            indexRequest_.unsetValues(kv.first);
        }

        return ret;
    }

    bool visitIndex(const Index& index) override {
        QueryVisitor::visitIndex(index);

        // Subselect the parts of the request
        datumRequest_ = indexRequest_;
        for (const auto& kv : index.key()) {
            datumRequest_.unsetValues(kv.first);
        }

        if (index.key().partialMatch(request_)) {
            return true; // Explore contained entries
        }
        return false; // Skip contained entries
    }

    void visitDatum(const Field& field, const Key& key) override {
        ASSERT(currentDatabase_);
        ASSERT(currentIndex_);

        if (key.match(datumRequest_)) {
            queue_.emplace(ListElement({currentDatabase_->key(), currentIndex_->key(), key},
                                          field.stableLocation()));
        }
    }

private: // methods
    metkit::MarsRequest indexRequest_;
    metkit::MarsRequest datumRequest_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif
