
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
#include "fdb5/database/WipeState.h"

#include <dirent.h>
#include <sys/stat.h>
#include <memory>

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/WipeState.h"

using namespace eckit;

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

WipeCatalogueVisitor::WipeCatalogueVisitor(eckit::Queue<CatalogueWipeState>& queue,
                                           const metkit::mars::MarsRequest& request, bool doit) :
    QueryVisitor<CatalogueWipeState>(queue, request), doit_(doit) {}


bool WipeCatalogueVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // If the Catalogue is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    catalogueWipeState_ = catalogue.wipeInit();

    if (doit_) {
        // Lock the database for everything but wiping.

        /// @note: the DAOS backends currently do not implement control identifiers -- actions cannot be disabled.
        /// @todo: what if the specific catalogue at hand (e.g. daos) does not technically require
        ///    locking list nor retrieve as there is no room for inconsistency? Must FDB::wipe be transactional
        ///    (i.e. only make visible the state before or after the operation has fully completed) or
        ///    are consumers allowed to access intermediate states?

        // Bind catalogue to the wipe state so that it can later restore the control state
        catalogueWipeState_.catalogue(catalogue.config());

        // Build the initial control state (is there really not a function for this?)
        ControlIdentifiers id;
        if (catalogue.enabled(ControlIdentifier::Archive)) {
            id |= ControlIdentifier::Archive;
        }
        if (catalogue.enabled(ControlIdentifier::List)) {
            id |= ControlIdentifier::List;
        }
        if (catalogue.enabled(ControlIdentifier::Retrieve)) {
            id |= ControlIdentifier::Retrieve;
        }
        catalogueWipeState_.initialControlState(id);

        catalogue.control(ControlAction::Disable,
                          ControlIdentifier::Archive | ControlIdentifier::Retrieve | ControlIdentifier::List);
    }

    // Having selected a DB, construct the residual request. This is the request that is used for
    // matching Index(es) -- which is relevant if there is subselection of the database.
    indexRequest_ = request_;
    for (const auto& kv : catalogue.key()) {
        indexRequest_.unsetValues(kv.first);
    }

    return true;  // Explore contained indexes
}

bool WipeCatalogueVisitor::visitIndex(const Index& index) {

    EntryVisitor::visitIndex(index);

    // Is this index matched by the supplied request?
    // n.b. If the request is over-specified (i.e. below the index level), nothing will be removed
    bool include = index.key().match(indexRequest_);

    include = currentCatalogue_->markIndexForWipe(index, include, catalogueWipeState_);

    // Enumerate data files
    for (auto& dataURI : index.dataURIs()) {
        if (include) {
            catalogueWipeState_.includeData(dataURI);
        }
        else {
            catalogueWipeState_.excludeData(dataURI);
        }
    }
    return true;
}

void WipeCatalogueVisitor::catalogueComplete(const Catalogue& catalogue) {

    ASSERT(currentCatalogue_ == &catalogue);

    catalogue.finaliseWipeState(catalogueWipeState_);
    catalogueWipeState_.sanityCheck();

    queue_.emplace(std::move(catalogueWipeState_));
    EntryVisitor::catalogueComplete(catalogue);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local