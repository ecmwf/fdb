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
#include "eckit/container/QueueOfQueues.h"
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

#include <memory>
#include <string>
#include <vector>

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

struct ListVisitor : public QueryVisitor<ListElement, eckit::QueueOfQueues<ListElement>> {

public:

    ListVisitor(eckit::QueueOfQueues<ListElement>& queue, const metkit::mars::MarsRequest& request, int level) :
        QueryVisitor<ListElement, eckit::QueueOfQueues<ListElement>>(queue, request), level_(level) {}

    bool supportsConcurrentIndexVisitation() const override { return true; }

    /// @todo remove this with better logic
    bool preVisitDatabase(const eckit::URI& uri, const Schema& schema) override {

        // If level == 1, avoid constructing the Catalogue/Store objects, so just interrogate the URIs
        if (level_ == 1 && uri.scheme() == "toc") {
            /// @todo only works with the toc backend
            if (auto dbKey = schema.matchDatabase(uri.path().baseName())) {
                emit(*dbKey, 0);
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
            emit(currentCatalogue_->key(), 0);
            ret = false;
        }

        return ret;
    }

    /// Subtracts the index key from the request, so we can test the request is used in its
    /// entirety. Returns null if the index does not match, avoiding enumeration of its entries.
    IndexScopePtr visitIndex(const Index& index, const Rule& rule, eckit::Queue<ListElement>& queue) override {

        // rule_ is the Datum rule (3rd level)
        // to match the index key, we need to canonicalise the request with the rule at Index level (2nd level) aka
        // rule_->parent()
        const metkit::mars::MarsRequest& canonicalRule = canonicalise(rule);

        if (!index.partialMatch(canonicalise(rule.parent()), canonicalRule)) {
            return nullptr;  // Skip contained entries
        }

        if (level_ == 2) {
            queue.emplace(currentCatalogue_->key(), index.key(), 0);
            return nullptr;
        }

        // Subselect the parts of the request
        metkit::mars::MarsRequest datumRequest = indexRequest_;
        for (const auto& kv : index.key()) {
            datumRequest.unsetValues(kv.first);
        }

        return std::make_unique<ListIndexScope>(*currentCatalogue_, index, rule, canonicalRule, queue,
                                                std::move(datumRequest));
    }

    using QueryVisitor<ListElement, eckit::QueueOfQueues<ListElement>>::visitDatum;
    /// Test if entry matches the current request. If so, add to the output queue.
    void visitDatum(IndexScope& indexScope, const Field& field, const Key& datumKey) override {
        ASSERT(currentCatalogue_);
        ASSERT(dynamic_cast<ListIndexScope*>(&indexScope));
        auto& scope = static_cast<ListIndexScope&>(indexScope);

        // Take into account any rule-specific behaviour in the request
        if (datumKey.partialMatch(scope.canonicalRule())) {
            for (const auto& k : datumKey.keys()) {
                scope.datumRequest().unsetValues(k);
            }
            if (scope.datumRequest().parameters().size() == 0) {
                scope.queue().emplace(currentCatalogue_->key(), scope.index().key(), datumKey, field.stableLocation(),
                                      field.timestamp());
            }
        }
    }

private:  // types

    /// Carries this index's place in the output: a sub-queue of the QueueOfQueues, taken in index
    /// order when the scope is claimed.
    class ListIndexScope : public IndexScope {

    public:  // methods

        ListIndexScope(const Catalogue& catalogue, const Index& index, const Rule& rule,
                       const metkit::mars::MarsRequest& canonicalRule, eckit::Queue<ListElement>& queue,
                       metkit::mars::MarsRequest&& datumRequest) :
            IndexScope(catalogue, index, rule),
            canonicalRule_(canonicalRule),
            queue_(queue),
            datumRequest_(std::move(datumRequest)) {}

        /// Closing releases this index's place in the output, so consumer can move onto the next index
        ~ListIndexScope() override { queue_.close(); }

        eckit::Queue<ListElement>& queue() { return queue_; }
        const metkit::mars::MarsRequest& canonicalRule() const { return canonicalRule_; }
        metkit::mars::MarsRequest& datumRequest() { return datumRequest_; }

    private:  // members

        const metkit::mars::MarsRequest& canonicalRule_;
        eckit::Queue<ListElement>& queue_;
        metkit::mars::MarsRequest datumRequest_;
    };

private:  // methods

    /// Outside of visiting an index, when the visitor has already obtained a Queue and stored
    /// it in a ListIndexScope, we need to create, and finalise, a sub-queue in the output
    /// QueueOfQueues to do any output.
    template <typename... Args>
    void emit(Args&&... args) {
        auto& queue = queue_.push();
        queue.emplace(std::forward<Args>(args)...);
        queue.close();
    }

private:  // members

    /// Read-only once set in visitDatabase(), before any index is visited.
    metkit::mars::MarsRequest indexRequest_;
    const int level_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

#endif
