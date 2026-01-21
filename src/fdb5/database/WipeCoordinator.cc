/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/WipeCoordinator.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb5 {

// ---------------------------------------------------------------------------------------------------


WipeElements WipeCoordinator::wipe(CatalogueWipeState& catalogueWipeState, bool doit, bool unsafeWipeAll) const {

    auto& storeWipeStates = catalogueWipeState.storeStates();

    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - processing store wipe states" << std::endl;

    // Contact each of the relevant stores.
    for (const auto& [storeURI, storeState] : storeWipeStates) {
        storeState->store(config_).finaliseWipeState(*storeState, doit, unsafeWipeAll);
    }

    UnknownsBuckets unknownURIs = gatherUnknowns(catalogueWipeState, storeWipeStates);

    bool unclean = !unknownURIs.catalogue.empty() || std::any_of(unknownURIs.store.begin(), unknownURIs.store.end(),
                                                                 [](const auto& pair) { return !pair.second.empty(); });

    if (doit && unclean && !unsafeWipeAll) {
        /// @todo: strictly speaking, we should be resetting the state anywhere we can throw too...
        /// Perhaps in the destructor of the wipe state?
        catalogueWipeState.resetControlState(catalogueWipeState.catalogue(config_));

        eckit::Log::warning() << "Unclean FDB database has the following unknown URIs:" << std::endl;
        for (const auto& uri : unknownURIs.catalogue) {
            eckit::Log::warning() << uri << std::endl;
        }
        for (const auto& [storeURI, uris] : unknownURIs.store) {
            for (auto& uri : uris) {
                eckit::Log::warning() << uri << std::endl;
            }
        }
        eckit::Log::warning() << "Wipe cannot proceed without --unsafe-wipe-all." << std::endl;

        throw eckit::Exception("Cannot fully wipe unclean FDB database");
    }

    if (doit && (!unclean || unsafeWipeAll)) {
        doWipeURIs(catalogueWipeState, storeWipeStates, unknownURIs, unsafeWipeAll);
    }

    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - completed" << std::endl;

    return generateWipeElements(catalogueWipeState, storeWipeStates, unknownURIs, unsafeWipeAll);
}

WipeCoordinator::UnknownsBuckets WipeCoordinator::gatherUnknowns(
    const CatalogueWipeState& catalogueWipeState,
    const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates) const {
    UnknownsBuckets unknowns;

    // 1) The catalogue should ignore URIs belonging to stores.
    for (const auto& uri : catalogueWipeState.unrecognisedURIs()) {
        bool found = false;
        for (const auto& [storeURI, storeState] : storeWipeStates) {
            if (storeState->store(config_).uriBelongs(uri)) {
                found = true;
                break;
            }
        }
        if (!found) {
            unknowns.catalogue.insert(uri);
        }
    }

    // 2) Each store should ignore URIs belonging to the catalogue.
    for (const auto& [storeURI, storeState] : storeWipeStates) {
        for (const auto& uri : storeState->unrecognisedURIs()) {
            if (catalogueWipeState.catalogue(config_).uriBelongs(uri)) {
                continue;
            }
            unknowns.store[storeURI].insert(uri);
        }
    }

    /// @todo: Test we are handling lock files, which the catalogue will own.

    return unknowns;
}

/// @todo: reduce URI copying here. We do not need the WipeState after this function call, so could move URIs out of it.
/// @todo: wiping on object storage will result in many data URIs being deleted and/or marked as safe, as every field is
///   kept in a separate object. This method could potentially generate a summary to keep the wipe report more concise.
WipeElements WipeCoordinator::generateWipeElements(
    CatalogueWipeState& catalogueWipeState,
    const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates, const UnknownsBuckets& unknownURIs,
    bool unsafeWipeAll) const {

    WipeElements report{};

    for (const auto& el : catalogueWipeState.extractWipeElements()) {
        report.push_back(el);
    }

    for (const auto& [storeURI, storeState] : storeWipeStates) {
        for (const auto& el : storeState->extractWipeElements()) {
            report.push_back(el);
        }
    }

    // Report unknowns
    std::set<eckit::URI> unknownURIsSet;
    for (const auto& uri : unknownURIs.catalogue) {
        unknownURIsSet.insert(uri);
    }
    for (const auto& [storeURI, uris] : unknownURIs.store) {
        for (const auto& uri : uris) {
            unknownURIsSet.insert(uri);
        }
    }

    if (!unknownURIsSet.empty()) {
        report.push_back(
            WipeElement(WipeElementType::UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet)));

        if (!unsafeWipeAll) {
            report.push_back(WipeElement(WipeElementType::ERROR, "Cannot fully wipe unclean FDB database:"));
        }
    }

    return report;
}

void WipeCoordinator::doWipeURIs(const CatalogueWipeState& catalogueWipeState,
                                 const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates,
                                 const UnknownsBuckets& unknownBuckets, bool unsafeWipeAll) const {

    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - DOIT! performing wipe operations" << std::endl;

    std::unique_ptr<Catalogue> catalogue =
        CatalogueReaderFactory::instance().build(catalogueWipeState.dbKey(), config_);

    // 1. Mask indexes
    /// @todo: This requires better test coverage...
    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - masking index entries" << std::endl;
    catalogue->maskIndexEntries(catalogueWipeState.indexesToMask());

    // 2. Wipe all database in a single operation if supported and appropriate
    /// @note: at this point we are already certain we can proceed with unsafe operations
    /// @note: this line creates a map which will tell which stores were not possible to wipe in one go
    std::map<eckit::URI, bool> storeWiped;
    for (const auto& [uri, storeState] : storeWipeStates) {
        storeWiped[uri] = false;
    }
    if (catalogueWipeState.safeURIs().empty()) {
        LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - attempting store wipe all" << std::endl;
        bool fullWipeSupported = true;
        for (const auto& [uri, storeState] : storeWipeStates) {
            const Store& store = storeState->store(config_);
            if (store.doUnsafeFullWipe()) {
                storeWiped[uri] = true;
            }
            else {
                fullWipeSupported = false;
            }
        }
        if (fullWipeSupported) {
            LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - attempting catalogue wipe all" << std::endl;
            if (catalogue->doUnsafeFullWipe()) {
                return;
            }
            /// @note: if stores support full wipe but catalogue does not, we proceed to
            /// wipe as usual, having the stores already wiped
        }
    }

    // 3. Wipe unknown files if unsafeWipeAll
    if (unsafeWipeAll) {
        LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - wiping unknown URIs" << std::endl;
        catalogue->doWipeUnknowns(unknownBuckets.catalogue);

        for (const auto& [uri, storeState] : storeWipeStates) {
            if (storeWiped[uri])
                continue;

            const Store& store = storeState->store(config_);

            auto it = unknownBuckets.store.find(uri);
            if (it != unknownBuckets.store.end()) {
                store.doWipeUnknowns(it->second);
            }
        }
    }

    // 4. Wipe files known by the stores
    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - wiping store known URIs" << std::endl;
    for (const auto& [uri, storeState] : storeWipeStates) {
        if (storeWiped[uri])
            continue;
        const Store& store = storeState->store(config_);
        store.doWipeURIs(*storeState);
    }

    // 5. Wipe files known by the catalogue
    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - wiping catalogue known URIs" << std::endl;
    catalogue->doWipeURIs(catalogueWipeState);

    // 6. wipe empty databases
    LOG_DEBUG_LIB(LibFdb5) << "WipeCoordinator::wipe - wiping empty databases" << std::endl;
    catalogue->doWipeEmptyDatabase();
    for (const auto& [uri, storeState] : storeWipeStates) {
        if (storeWiped[uri])
            continue;
        const Store& store = storeState->store(config_);
        store.doWipeEmptyDatabase();
    }
}

}  // namespace fdb5