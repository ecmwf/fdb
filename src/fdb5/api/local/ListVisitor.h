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

#ifndef fdb5_api_local_ListVisitor_H
#define fdb5_api_local_ListVisitor_H

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/ListIterator.h"

namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct ListVisitor : public QueryVisitor<ListElement> {

public:
    using QueryVisitor<ListElement>::QueryVisitor;

    /// Make a note of the current database. Subtract its key from the current
    /// request so we can test request is used in its entirety
//    bool visitDatabase(const Catalogue& catalogue, const Store& store) override {
    bool visitDatabase(const Catalogue& catalogue) override {

        // If the DB is locked for listing, then it "doesn't exist"
        if (!catalogue.enabled(ControlIdentifier::List)) {
            return false;
        }

//        bool ret = QueryVisitor::visitDatabase(catalogue, store);
        bool ret = QueryVisitor::visitDatabase(catalogue);
        ASSERT(catalogue.key().partialMatch(request_));

        // Subselect the parts of the request
        indexRequest_ = request_;
        for (const auto& kv : catalogue.key()) {
            indexRequest_.unsetValues(kv.first);
        }

        return ret;
    }

    /// Make a note of the current database. Subtracts key from the current request
    /// so we can test request is used in its entirety.
    ///
    /// Returns true/false depending on matching the request (avoids enumerating
    /// entries if not matching).
    bool visitIndex(const Index& index) override {
        QueryVisitor::visitIndex(index);

        // Subselect the parts of the request
        datumRequest_ = indexRequest_;
        for (const auto& kv : index.key()) {
            datumRequest_.unsetValues(kv.first);
        }

        if (index.partialMatch(request_)) {
            return true; // Explore contained entries
        }
        return false; // Skip contained entries
    }

    /// Test if entry matches the current request. If so, add to the output queue.
    void visitDatum(const Field& field, const TypedKey& datumKey) override {
        ASSERT(currentCatalogue_);
        ASSERT(currentIndex_);

        if (datumKey.match(datumRequest_)) {
            queue_.emplace(ListElement({currentCatalogue_->key(), currentIndex_->key(), datumKey.canonical()}, field.stableLocation(), field.timestamp()));
        }
    }

    void visitDatum(const Field& field, const std::string& keyFingerprint) override {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }

private: // members

    metkit::mars::MarsRequest indexRequest_;
    metkit::mars::MarsRequest datumRequest_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif
