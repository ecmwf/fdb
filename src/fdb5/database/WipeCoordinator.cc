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
#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb5 {

// Use WipeState from the Catalogue to assign safe and data URIs to the corresponding stores
std::map<eckit::URI, std::unique_ptr<StoreWipeState>> WipeCoordinator::getStoreStates(
    const WipeState& wipeState) const {
    std::map<eckit::URI, std::unique_ptr<StoreWipeState>> storeWipeStates;

    auto storeState = [&](const eckit::URI& storeURI) -> StoreWipeState& {
        auto [it, inserted] = storeWipeStates.try_emplace(storeURI, nullptr);
        if (inserted || !it->second) {
            it->second = std::make_unique<StoreWipeState>(it->first);
        }
        return *it->second;
    };

    for (const auto& dataURI : wipeState.includeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).include(dataURI);
    }

    for (const auto& dataURI : wipeState.excludeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).exclude(dataURI);
    }

    return storeWipeStates;
}

// ---------------------------------------------------------------------------------------------------

WipeCoordinator::UnknownsBuckets WipeCoordinator::gatherUnknowns(
    const CatalogueWipeState& catalogueWipeState,
    const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates) const
{
    UnknownsBuckets unknowns;

    // 1) URIs the catalogue doesn't recognise AND no store claims -> "nobody"
    for (const auto& uri : catalogueWipeState.unrecognisedURIs()) {
        bool found = false;
        for (const auto& [storeURI, storeState] : storeWipeStates) {
            if (storeState->ownsURI(uri)) { found = true; break; }
        }
        if (!found) {
            unknowns.catalogue.insert(uri);
        }
    }

    // 2) For each store’s own "unrecognised" URIs, if catalogue doesn't own, bucket them under that store
    //    NB: we are not checking if another store might own these (same assumption as your current code).
    for (const auto& [storeURI, storeState] : storeWipeStates) {
        for (const auto& uri : storeState->unrecognisedURIs()) {
            if (catalogueWipeState.ownsURI(uri)) continue;
            unknowns.store[storeURI].insert(uri);
        }
    }

    return unknowns;
}


// Create WipeElements to report to the user what is going to be wiped.
// NB: This copies every URI involved in the wipe.
std::unique_ptr<CatalogueWipeState> WipeCoordinator::generateReport(const CatalogueWipeState& catalogueWipeState, const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates, const UnknownsBuckets& unknownURIs,
    bool unsafeWipeAll
) const {
    auto report = std::make_unique<CatalogueWipeState>(catalogueWipeState.dbKey());

    for (const auto& el : catalogueWipeState.generateWipeElements()) {
        report->insertWipeElement(el);
    }

    for (const auto& [storeURI, storeState] : storeWipeStates) {
        for (const auto& el : storeState->generateWipeElements()) {
            report->insertWipeElement(el);
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
        report->insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_UNKNOWN,
                                                "Unexpected entries in FDB database:", std::move(unknownURIsSet)));

        if (!unsafeWipeAll) {
            report->insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:"));
        }

    }

    return report;
}

// Almost certainly std::unique_ptr<CatalogueWipeState> can just be CatalogueWipeState everywhere...
std::unique_ptr<CatalogueWipeState> WipeCoordinator::wipe(const CatalogueWipeState& catalogueWipeState, bool doit,
                                                          bool unsafeWipeAll) const {

    auto storeWipeStates = catalogueWipeState.takeStoreStates();
    ASSERT(!storeWipeStates.empty());  // XXX: We should handle case where there is no work for the stores... Is there such a case?

    eckit::Log::info() << "WipeCoordinator::wipe - processing store wipe states" << std::endl;

    // Contact each of the relevant stores.
    for (const auto& [storeURI, storeState] : storeWipeStates) {
        storeState->store(config_).prepareWipe(*storeState);
    }

    UnknownsBuckets unknownURIs = gatherUnknowns(catalogueWipeState, storeWipeStates);

    bool unclean = !unknownURIs.catalogue.empty() || std::any_of(unknownURIs.store.begin(), unknownURIs.store.end(),
        [](const auto& pair) { return !pair.second.empty(); });

    auto report = generateReport(catalogueWipeState, storeWipeStates, unknownURIs, unsafeWipeAll);
    
    //  XXX todo: this should probably be reported via wipe element
    const auto& indexesToMask = catalogueWipeState.indexesToMask();
    if (!indexesToMask.empty()) {
        eckit::Log::info() << "Masking indexes: " << std::endl;
        for (const auto& index : indexesToMask) {
            eckit::Log::info() << index << std::endl;  // !! issues on remote fdb?
        }
    }

    if (doit && unclean && !unsafeWipeAll ) {
        throw eckit::Exception("Cannot fully wipe unclean FDB database");
    }

    if (doit && (!unclean || unsafeWipeAll)) {  // I'd love to put this in a doit function, called from outside.
        doWipe(
            catalogueWipeState,
            storeWipeStates,
            unknownURIs,
            unsafeWipeAll
        );
    }

    eckit::Log::info() << "WipeCoordinator::wipe - completed" << std::endl;

    return report;
}


// Maybe it would be possible for this to operate on a single wipe state object, which is the object we would otherwise use for reporting.
// This does at least make sure we are reporting exactly what we are wiping. But it also means WipeState is doing a bit too much.

// We shall do the following, in order:
// - 1. mask any indexes that need masking.
// - 2. wipe the unknown files in catalogue and stores.
// - 3. wipe the files known by the stores.
// - 4. wipe the files known by the catalogue.
// - 5. wipe empty databases.

void WipeCoordinator::doWipe(
    const CatalogueWipeState& catalogueWipeState, 
    const std::map<eckit::URI, std::unique_ptr<StoreWipeState>>& storeWipeStates,
    const UnknownsBuckets& unknownBuckets,
    bool unsafeWipeAll) const {
    
    eckit::Log::info() << "WipeCoordinator::wipe - DOIT! performing wipe operations" << std::endl;

    std::unique_ptr<Catalogue> catalogue =
        CatalogueReaderFactory::instance().build(catalogueWipeState.dbKey(), config_);

    // 1. XXX Mask indexes TODO
    // eckit::Log::info() << "WipeCoordinator::wipe - masking indexes" << std::endl;
    for (const auto& index : catalogueWipeState.indexesToMask()) {
        //     catalogue->maskIndexEntry(index); // This is way too chatty for remote fdb. This should be one
        //     function call. (Also, handler knows what indexes to match, probably, unless there's been an update.)
    }

    // 2. Wipe unknown files
    // What was my reason for doing the unknowns first? I think it would be better to do the knowns.
    eckit::Log::info() << "WipeCoordinator::wipe - wiping unknown URIs" << std::endl;

    //XXX: We probably shouldn't bother calling these functions if there are no unknown URIs.
    catalogue->wipeUnknown(unknownBuckets.catalogue);

    for (const auto& [uri, storeState] : storeWipeStates) {
        const Store& store = storeState->store(config_);

        auto it = unknownBuckets.store.find(uri);
        if (it != unknownBuckets.store.end()) {
            store.doWipeUnknownContents(it->second);
        }
    }

    // 3. Wipe files known by the stores
    eckit::Log::info() << "WipeCoordinator::wipe - wiping store known URIs" << std::endl;
    for (const auto& [uri, storeState] : storeWipeStates) {
        const Store& store = storeState->store(config_);
        store.doWipe(*storeState);
    }

    // 4. Wipe files known by the catalogue
    eckit::Log::info() << "WipeCoordinator::wipe - wiping catalogue known URIs" << std::endl;
    catalogue->doWipe(catalogueWipeState);

    // 5. wipe empty databases
    eckit::Log::info() << "WipeCoordinator::wipe - wiping empty databases" << std::endl;
    catalogue->doWipeEmptyDatabases();
    for (const auto& [uri, storeState] : storeWipeStates) {
        const Store& store = storeState->store(config_);
        store.doWipeEmptyDatabases();
    }
}


} // namespace fdb5