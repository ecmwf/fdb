/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <memory>
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/WipeState.h"

namespace fdb5 {

// Class for coordinating fdb.wipe across catalogue and stores
class WipeCoordinator {
public:

    struct UnknownsBuckets {
        std::map<eckit::URI, std::set<eckit::URI>> store;  // store -> URIs
        std::set<eckit::URI> catalogue;
    };

public:

    WipeCoordinator(const Config& config) : config_(config) {}

    /// Perform a wipe operation on the provided wipe state.
    /// URIs will only be deleted if 'doit' is true.
    WipeElements wipe(CatalogueWipeState& catalogueState, bool doit, bool unsafeWipeAll) const;

private:


    /// Identify URIs unknown to the catalogue and the stores
    UnknownsBuckets gatherUnknowns(const CatalogueWipeState& catalogueWipeState,
                                   const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates) const;

    /// Create WipeElements to report to the user what is going to be wiped.
    WipeElements generateWipeElements(CatalogueWipeState& catalogueWipeState,
                                      const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates,
                                      const UnknownsBuckets& unknownURIs, bool unsafeWipeAll) const;

    /// Delete the data as per the wipe states.
    void doWipeURIs(const CatalogueWipeState& catalogueWipeState,
                    const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates,
                    const UnknownsBuckets& unknownURIs, bool unsafeWipeAll) const;

private:

    const Config& config_;
};

}  // namespace fdb5
