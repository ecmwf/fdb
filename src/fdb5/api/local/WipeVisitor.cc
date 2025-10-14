
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

#include "eckit/filesystem/URI.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/api/local/WipeVisitor.h"

#include <dirent.h>
#include <iostream>
#include <memory>
#include <sys/stat.h>

#include "eckit/os/Stat.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"

using namespace eckit;

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

WipeCatalogueVisitor::WipeCatalogueVisitor(eckit::Queue<std::unique_ptr<WipeState>>& queue, const metkit::mars::MarsRequest& request, bool doit,
                         bool porcelain, bool unsafeWipeAll) :
    QueryVisitor<std::unique_ptr<WipeState>>(queue, request), doit_(doit), porcelain_(porcelain), unsafeWipeAll_(unsafeWipeAll) {}


bool WipeCatalogueVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // If the Catalogue is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    // // Check that we are in a clean state (i.e. we only visit one DB).
    // ASSERT(stores_.empty());
    // ASSERT(catalogue.wipeElements().empty());
    // ASSERT(catalogueWipeElements_.empty());
    // ASSERT(storeWipeElements_.empty());

    // @todo: Catalogue specific checks
    // wipeElements = catalogue.wipeInit();

    ASSERT(!catalogueWipeState_);
    catalogueWipeState_ = catalogue.wipeInit();
    if (!catalogueWipeState_) {
    //     for (const auto& el : catalogueWipeState_->wipeElements()) {
    //         queue_.push(*el); // but isnt this empty atm?
    //     }

        // Log::error() << "Catalogue " << catalogue.key() << " cannot be wiped." << std::endl;
        // auto errorIt = catalogueWipeElements_.find(WipeElementType::WIPE_ERROR);
        // if (errorIt != catalogueWipeElements_.end() && !errorIt->second.empty()) {
        //     for (const auto& el : errorIt->second) {
        //         Log::error() << "Error: " << el.msg() << std::endl;
        //         for (const auto& uri : el.uris()) {
        //             Log::error() << "    " << uri << std::endl;
        //         }
        //     }
        // }

        // If the catalogue cannot be wiped, then we don't proceed.
        // This is a safeguard against wiping a catalogue that is not in a state to be wiped.
        return false;
    }

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the database.
    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

void WipeCatalogueVisitor::aggregateURIs(const eckit::URI& dataURI, bool include, WipeState& wipeState) {

    if (include) {
        wipeState.include(dataURI);
    }
    else {
        wipeState.exclude(dataURI);
    }
}

bool WipeCatalogueVisitor::visitIndex(const Index& index) {

    EntryVisitor::visitIndex(index);

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed
    bool include = index.key().match(indexRequest_);

    include = currentCatalogue_->wipeIndex(index, include, *catalogueWipeState_);

    // Enumerate data files
    for (auto& dataURI : index.dataURIs()) {
        aggregateURIs(dataURI, include, *catalogueWipeState_);
    }
    return true;
}

void WipeCatalogueVisitor::catalogueComplete(const Catalogue& catalogue) {

    // We will be aggregating the following:
    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;
    std::map<WipeElementType, std::shared_ptr<WipeElement>> storeElements;

    ASSERT(currentCatalogue_ == &catalogue);

    auto maskedDataPaths = catalogue.wipeFinialise(*catalogueWipeState_); // why returning uris
    for (const auto& uri : maskedDataPaths) {
        /// XXX : Why are these always included?
        aggregateURIs(uri, true, *catalogueWipeState_);
    }
    
    queue_.emplace(std::move(catalogueWipeState_));
    EntryVisitor::catalogueComplete(catalogue);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local