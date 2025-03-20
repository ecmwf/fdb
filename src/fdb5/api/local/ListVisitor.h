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

#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/types/Type.h"

#include "metkit/mars/MarsRequest.h"

#include <string>
#include <vector>

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct ListVisitor : public QueryVisitor<ListElement> {

public:

    ListVisitor(eckit::Queue<ListElement>& queue, const metkit::mars::MarsRequest& request, int level) :
        QueryVisitor<ListElement>(queue, request), level_(level) {}

    /// @todo remove this with better logic
    bool preVisitDatabase(const eckit::URI& uri, const Schema& schema) override {

        // If level == 1, avoid constructing the Catalogue/Store objects, so just interrogate the URIs
        if (level_ == 1 && uri.scheme() == "toc") {
            /// @todo only works with the toc backend
            if (auto dbKey = schema.matchDatabase(uri.path().baseName())) {
                queue_.emplace(*dbKey, 0);
                return false;
            }
        }
        return true;
    }

    /// Make a note of the current database. Subtract its key from the current
    /// request so we can test request is used in its entirety
    bool visitDatabase(const Catalogue& catalogue) override {

        // If the DB is locked for listing, then it "doesn't exist"
        if (!catalogue.enabled(ControlIdentifier::List)) {
            return false;
        }

        bool ret = QueryVisitor::visitDatabase(catalogue);

        if (!currentCatalogue_->key().partialMatch(canonicalise(catalogue.rule()))) {
            return false;
        }

        // Subselect the parts of the request
        indexRequest_ = request_;
        for (const auto& [k, v] : currentCatalogue_->key()) {
            indexRequest_.unsetValues(k);
        }

        if (level_ == 1) {
            queue_.emplace(currentCatalogue_->key(), 0);
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

        // rule_ is the Datum rule (3rd level)
        // to match the index key, we need to canonicalise the request with the rule at Index level (2nd level) aka
        // rule_->parent()
        if (index.partialMatch(canonicalise(rule_->parent()), canonicalise(*rule_))) {

            // Subselect the parts of the request
            datumRequest_ = indexRequest_;

            for (const auto& kv : index.key()) {
                datumRequest_.unsetValues(kv.first);
            }

            if (level_ == 2) {
                queue_.emplace(currentCatalogue_->key(), currentIndex_->key(), 0);
                return false;
            }

            return true;  // Explore contained entries
        }

        return false;  // Skip contained entries
    }

    /// Test if entry matches the current request. If so, add to the output queue.
    void visitDatum(const Field& field, const Key& datumKey) override {
        ASSERT(currentCatalogue_);
        ASSERT(currentIndex_);

        // Take into account any rule-specific behaviour in the request
        if (datumKey.partialMatch(canonicalise(*rule_))) {
            for (const auto& k : datumKey.keys()) {
                datumRequest_.unsetValues(k);
            }
            if (datumRequest_.parameters().size() == 0) {
                queue_.emplace(currentCatalogue_->key(), currentIndex_->key(), datumKey, field.stableLocation(),
                               field.timestamp());
            }
        }
    }

    void visitDatum(const Field& field, const std::string& keyFingerprint) override {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }

private:  // members

    metkit::mars::MarsRequest indexRequest_;
    metkit::mars::MarsRequest datumRequest_;
    const int level_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

#endif
