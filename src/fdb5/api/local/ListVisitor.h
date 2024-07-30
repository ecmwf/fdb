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

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "eckit/os/BackTrace.h"

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct ListVisitor : public QueryVisitor<ListElement> {

public:
    ListVisitor(eckit::Queue<ListElement>& queue, const metkit::mars::MarsRequest& request, int level):
        QueryVisitor<ListElement>(queue, request), level_(level) { }

    /// Make a note of the current database. Subtract its key from the current
    /// request so we can test request is used in its entirety
    bool visitDatabase(const Catalogue& catalogue, const Store& store) override {

        // If the DB is locked for listing, then it "doesn't exist"
        if (!catalogue.enabled(ControlIdentifier::List)) {
            return false;
        }

        bool ret = QueryVisitor::visitDatabase(catalogue, store);
        ASSERT(catalogue.key().partialMatch(request_));

        // Subselect the parts of the request
        indexRequest_ = request_;
        for (const auto& kv : catalogue.key()) {
            indexRequest_.unsetValues(kv.first);
        }

        if (level_ == 1) {
            /// @todo fix this so that we don't need to create an empty key
            const auto emptyKey = Key {currentCatalogue_->schema().registry()};
            queue_.emplace(ListElement({currentCatalogue_->key(), emptyKey, emptyKey}, FieldLocation::nullLocation(), 0));
            ret = false;
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
            if (level_ == 2) {
                /// @todo fix this so that we don't need to create an empty key
                const auto emptyKey = Key {currentCatalogue_->schema().registry()};
                const auto keyParts = std::vector<Key> {currentCatalogue_->key(), currentIndex_->key(), emptyKey};
                queue_.emplace(ListElement(keyParts, FieldLocation::nullLocation(), 0));
                return false;
            }
            return true;  // Explore contained entries
        }

        return false;  // Skip contained entries
    }

    /// Test if entry matches the current request. If so, add to the output queue.
    void visitDatum(const Field& field, const Key& key) override {
        ASSERT(currentCatalogue_);
        ASSERT(currentIndex_);

        if (key.match(datumRequest_)) {
            queue_.emplace(ListElement({currentCatalogue_->key(), currentIndex_->key(), key}, field.stableLocation(), field.timestamp()));
        }
    }

    void visitDatum(const Field& field, const std::string& keyFingerprint) override {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }

private: // members

    metkit::mars::MarsRequest indexRequest_;
    metkit::mars::MarsRequest datumRequest_;
    const int level_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

#endif
