/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/io/AutoCloser.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Manager.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

#include <memory>
#include <vector>

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBVisitException : public eckit::Exception {
public:

    using Exception::Exception;
};

//----------------------------------------------------------------------------------------------------------------------

bool EntryVisitor::preVisitDatabase(const eckit::URI& /*uri*/, const Schema& /*schema*/) {
    return true;
}

EntryVisitor::EntryVisitor() : currentCatalogue_(nullptr), currentStore_(nullptr), currentIndex_(nullptr) {}

EntryVisitor::~EntryVisitor() {
    delete currentStore_;
}

Store& EntryVisitor::store() const {
    if (!currentStore_) {
        ASSERT(currentCatalogue_);
        currentStore_ = currentCatalogue_->buildStore().release();
        ASSERT(currentStore_);
    }
    return *currentStore_;
}

bool EntryVisitor::visitDatabase(const Catalogue& catalogue) {
    currentCatalogue_ = &catalogue;
    currentStore_ = nullptr;
    currentIndex_ = nullptr;
    rule_ = &currentCatalogue_->rule();
    return true;
}

void EntryVisitor::catalogueComplete(const Catalogue& catalogue) {
    if (currentCatalogue_ != nullptr) {
        ASSERT(currentCatalogue_ == &catalogue);
    }
    currentCatalogue_ = nullptr;
    delete currentStore_;
    currentStore_ = nullptr;
    currentIndex_ = nullptr;
    rule_ = nullptr;
}

bool EntryVisitor::visitIndex(const Index& index) {
    ASSERT(currentCatalogue_);
    currentIndex_ = &index;
    rule_ = &currentCatalogue_->schema().matchingRule(currentCatalogue_->key(), currentIndex_->key());
    return true;
}

void EntryVisitor::visitDatum(const Field& field, const std::string& keyFingerprint) {
    ASSERT(currentCatalogue_);
    ASSERT(currentIndex_);

    const auto datumKey = rule_->makeKey(keyFingerprint);

    visitDatum(field, datumKey);
}

time_t EntryVisitor::indexTimestamp() const {
    return currentIndex_ == nullptr ? 0 : currentIndex_->timestamp();
}

//----------------------------------------------------------------------------------------------------------------------

EntryVisitMechanism::EntryVisitMechanism(const Config& config) : dbConfig_(config), fail_(true) {}

void EntryVisitMechanism::visit(const FDBToolRequest& request, EntryVisitor& visitor) {

    if (visitor.visitEntries() && !visitor.visitIndexes()) {
        throw FDBVisitException("Cannot visit entries without visiting indexes", Here());
    }

    // A request against all is the same as using an empty key in visitableLocations.

    ASSERT(request.all() == request.request().empty());

    /// @todo Put minimim keys check into FDBToolRequest.

    LOG_DEBUG_LIB(LibFdb5) << "REQUEST ====> " << request.request() << std::endl;

    try {
        fdb5::Manager mg{dbConfig_};
        std::vector<URI> uris(mg.visitableLocations(request.request(), request.all()));

        // n.b. it is not an error if nothing is found (especially in a sub-fdb).

        // And do the visitation
        for (const URI& uri : uris) {
            if (!visitor.preVisitDatabase(uri, dbConfig_.schema())) {
                continue;
            }

            /// @note: the schema of a URI returned by visitableLocations
            ///   matches the corresponding Engine type name
            // fdb5::Engine& ng = fdb5::Engine::backend(uri.scheme());
            LOG_DEBUG_LIB(LibFdb5) << "FDB processing URI " << uri << std::endl;

            std::unique_ptr<CatalogueReader> catalogue;
            try {

                catalogue = CatalogueReaderFactory::instance().build(uri, dbConfig_);
            }
            catch (fdb5::DatabaseNotFoundException& e) {
                visitor.onDatabaseNotFound(e);
            }

            ASSERT(catalogue->open());

            eckit::AutoCloser<Catalogue> closer(*catalogue);

            catalogue->visitEntries(visitor, /* *store, */ false);
        }
    }
    catch (eckit::UserError&) {
        throw;
    }
    catch (eckit::Exception& e) {
        Log::warning() << e.what() << std::endl;
        if (fail_) {
            throw;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
