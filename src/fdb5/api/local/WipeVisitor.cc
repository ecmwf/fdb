
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

#include "fdb5/api/local/WipeVisitor.h"

#include <dirent.h>
#include <sys/stat.h>

#include "eckit/os/Stat.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"

using namespace eckit;

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

WipeVisitor::WipeVisitor(eckit::Queue<WipeElement>& queue, const metkit::mars::MarsRequest& request, bool doit,
                         bool porcelain, bool unsafeWipeAll) :
    QueryVisitor<WipeElement>(queue, request),
    doit_(doit),                                        
    porcelain_(porcelain),
    unsafeWipeAll_(unsafeWipeAll) {}


bool WipeVisitor::visitDatabase(const Catalogue& catalogue) {

    // If the Catalogue is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    EntryVisitor::visitDatabase(catalogue);
    
    // // Check that we are in a clean state (i.e. we only visit one DB).
    ASSERT(stores_.empty());
    ASSERT(catalogueWipeElements_.empty());
    ASSERT(storeWipeElements_.empty());

    // @todo: Catalogue specific checks
    if (!catalogue.wipeInit()) {
        Log::error() << "Catalogue " << catalogue.key() << " cannot be wiped." << std::endl;
        auto errorIt = catalogueWipeElements_.find(WipeElementType::WIPE_CATALOGUE_ERROR);
        if (errorIt != catalogueWipeElements_.end() && !errorIt->second.empty()) {
            for (const auto& el : errorIt->second) {
                Log::error() << "Error: " << el.msg() << std::endl;
                for (const auto& uri : el.uris()) {
                    Log::error() << "    " << uri << std::endl;
                }
            }
        }

        // If the catalogue cannot be wiped, then we don't proceed.
        // This is a safeguard against wiping a catalogue that is not in a state to be wiped.
        return false;
    }

    // check for errors

    // ASSERT(subtocPaths_.empty());
    // ASSERT(lockfilePaths_.empty());
    // ASSERT(indexPaths_.empty());
    // ASSERT(safeCataloguePaths_.empty());
    // ASSERT(indexesToMask_.empty());

    // ASSERT(!tocPath_.asString().size());
    // ASSERT(!schemaPath_.asString().size());

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the database.
    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

void WipeVisitor::storeURI(const eckit::URI& dataURI) {

    auto storeURI = StoreFactory::instance().uri(dataURI);
    auto storeIt = stores_.find(storeURI);
    if (storeIt == stores_.end()) {
        auto store = StoreFactory::instance().build(storeURI, currentCatalogue_->config());
        ASSERT(store);
        storeIt = stores_.emplace(storeURI, std::move(store)).first;
    }
    auto dataURIsIt = dataURIbyStore_.find(storeURI);
    if (dataURIsIt == dataURIbyStore_.end()) {
        dataURIsIt = dataURIbyStore_.emplace(storeURI, std::vector<eckit::URI>{}).first; 
    }
    dataURIsIt->second.push_back(dataURI);
}

bool WipeVisitor::visitIndex(const Index& index) {

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed
    bool include = index.key().match(indexRequest_);

    include = currentCatalogue_->wipe(index, include);

    // Enumerate data files
    for (auto& dataURI : index.dataURIs()) {
        storeURI(dataURI);
    }
    return false;
}

void WipeVisitor::catalogueComplete(const Catalogue& catalogue) {

    ASSERT(currentCatalogue_ == &catalogue);

    auto& dataURI = currentCatalogue_->wipeFinish();
    for (const auto& uri : dataURI) {
        storeURI(uri);
    }

    bool canWipe = true;
    for (const auto& [storeURI, dataURIs] : dataURIbyStore_) {
        auto storeIt = stores_.find(storeURI);
        ASSERT(storeIt != stores_.end());

        canWipe = canWipe && storeIt->second->canWipe(dataURIs, unsafeWipeAll_);
    }



    EntryVisitor::catalogueComplete(catalogue);
}

// bool WipeVisitor::visitIndexes() {
//     ASSERT(internalVisitor_);
//     return internalVisitor_->visitIndexes();
// }
//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local