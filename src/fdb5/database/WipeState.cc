#include "fdb5/database/WipeState.h"

namespace fdb5 {

// Use WipeState from the Catalogue to assign safe and data URIs to the corresponding stores
std::map<eckit::URI, std::unique_ptr<StoreWipeState>> WipeCoordinator::getStoreStates(const WipeState& wipeState) const {
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


// todo: break this logic up a bit more, for readability.
void WipeCoordinator::wipe(eckit::Queue<WipeElement>& queue, const CatalogueWipeState& catalogueWipeState, bool doit,
                           bool unsafeWipeAll) const {

    bool error   = false;  ///
    bool canWipe = true;   /// !!! unused.
    bool wipeAll = true;   /// ?
    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;
    std::map<WipeElementType, std::shared_ptr<WipeElement>> storeElements;

    // Gather unknowns from the catalogue
    eckit::Log::info() << "WipeCoordinator::wipe - gathering unknownURIs from catalogueWipeState" << std::endl;
    for (const auto& el : catalogueWipeState.wipeElements()) {
        if (el->type() == WipeElementType::WIPE_CATALOGUE_SAFE && !el->uris().empty()) {
            wipeAll = false;
        }
        // if wipeAll, collect all the unknown URIs
        if (wipeAll && el->type() == WipeElementType::WIPE_UNKNOWN && !el->uris().empty()) {
            for (const auto& uri : el->uris()) {
                unknownURIs.emplace(uri, std::nullopt);
            }
        }
    }

    eckit::Log::info() << "WipeCoordinator::wipe - gathering store wipe states" << std::endl;
    auto storeWipeStates = getStoreStates(catalogueWipeState);
    ASSERT(!storeWipeStates.empty());

    eckit::Log::info() << "WipeCoordinator::wipe - processing store wipe states" << std::endl;
    for (const auto& [storeURI, storeStatePtr] : storeWipeStates) {
        auto& storeState = *storeStatePtr;

        if (!storeState.excludeURIs().empty()) {
            wipeAll = false;
        }

        // This could act on the storeWipeState directly.
        auto elements = storeState.store(config_).prepareWipe(storeState.includeURIs(), storeState.excludeURIs(), wipeAll);

        for (const auto& el : elements) {
            storeState.insertWipeElement(el);  // could be done in prepare wipe.

            const auto type = el->type();

            // If wipeAll, collect all the unknown URIs
            if (wipeAll && type == WipeElementType::WIPE_UNKNOWN) {
                for (const auto& uri : el->uris()) {
                    unknownURIs.try_emplace(uri, storeURI);
                }
                continue;
            }

            // Group elements by type, merging URIs if the type already exists
            auto [it, inserted] = storeElements.emplace(type, el);
            if (!inserted) {
                auto& target = *it->second;
                for (const auto& uri : el->uris()) {
                    target.add(uri);
                }
            }
        }
    }

    auto isCatalogueElement = [](const auto& el) {
        const auto t = el->type();
        return (t == WipeElementType::WIPE_CATALOGUE || t == WipeElementType::WIPE_CATALOGUE_AUX) &&
               !el->uris().empty();
    };

    auto isStoreElement = [](WipeElementType t) {
        return t == WipeElementType::WIPE_STORE || t == WipeElementType::WIPE_STORE_AUX;
    };


    ASSERT(storeElements.size() != 0);
    if (wipeAll && unknownURIs.size() > 0) {

        auto it = unknownURIs.begin();
        while (it != unknownURIs.end()) {

            const auto& uri = it->first;

            // Remove uri from the unknowns if it is among the catalogue wipe elements
            bool found = false;
            for (const auto& el : catalogueWipeState.wipeElements()) {
                if (isCatalogueElement(el) && el->uris().count(uri)) {
                    it    = unknownURIs.erase(it);
                    found = true;
                    break;
                }
            }

            if (found) {
                continue;
            }

            for (const auto& [type, el] : storeElements) {
                if (isStoreElement(type) && el->uris().count(uri)) {
                    it    = unknownURIs.erase(it);
                    found = true;
                    break;
                }
            }

            if (!found) {
                ++it;  // Move to the next URI
            }
        }
    }

    // PUSHING TO THE REPORT QUEUE
    eckit::Log::info() << "WipeCoordinator::wipe - pushing wipe elements to report queue" << std::endl;
    if (wipeAll && doit && !unknownURIs.empty() && !unsafeWipeAll) {
        WipeElement el(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
        queue.push(el);
        error = true;
    }

    // gather all non-unknown elements from the catalogue and the stores
    for (const auto& el : catalogueWipeState.wipeElements()) {
        if (el->type() != WipeElementType::WIPE_UNKNOWN) {
            queue.push(*el);
        }
    }

    for (const auto& [type, el] : storeElements) {
        if (type != WipeElementType::WIPE_UNKNOWN) {
            queue.push(*el);
        }
    }

    // gather the unknowns.
    // This can surely be done better...
    eckit::Log::info() << "WipeCoordinator::wipe - gathering unknown URIs" << std::endl;
    std::set<eckit::URI> unknownURIsSet;
    std::vector<eckit::URI> unknownURIsCatalogue;
    std::map<eckit::URI, std::vector<eckit::URI>> unknownURIsStore;
    for (const auto& [uri, storeURI] : unknownURIs) {
        unknownURIsSet.insert(uri);

        if (storeURI) {
            auto it = unknownURIsStore.find(*storeURI);
            if (it == unknownURIsStore.end()) {
                unknownURIsStore.emplace(*storeURI, std::vector<eckit::URI>{uri});
            }
            else {
                it->second.push_back(uri);
            }
        }
        else {
            unknownURIsCatalogue.push_back(uri);
        }
    }

    queue.emplace(WipeElementType::WIPE_UNKNOWN, "Unexpected entries in FDB database:", std::move(unknownURIsSet));

    
    // todo: this should probably be reported via wipe element
    const auto& indexesToMask = catalogueWipeState.indexesToMask();
    if (!indexesToMask.empty()) {
        eckit::Log::info() << "Masking indexes: " << std::endl;
        for (const auto& index : indexesToMask) {
            eckit::Log::info() << index << std::endl; // !! issues on remote fdb?
        }
    }
    
    if (error && doit) {
        ASSERT(false); /// xxx:: actual error
        return;
    }

    if (doit && !error) {
        eckit::Log::info() << "WipeCoordinator::wipe - DOIT! performing wipe operations" << std::endl;
        // We shall do the following, in order:
        // - 1. mask any indexes that need masking.
        // - 2. wipe the unknown files in catalogue and stores.
        // - 3. wipe the files known by the stores.
        // - 4. wipe the files known by the catalogue.
        // - 5. wipe empty databases.
        

        std::unique_ptr<Catalogue> catalogue = CatalogueReaderFactory::instance().build(catalogueWipeState.dbkey(), config_);

        // 1. Mask indexes
        eckit::Log::info() << "WipeCoordinator::wipe - masking indexes" << std::endl;
        for (const auto& index : catalogueWipeState.indexesToMask()) {
            catalogue->maskIndexEntry(index);
        }

        // 2. Wipe unknown files
        eckit::Log::info() << "WipeCoordinator::wipe - wiping unknown URIs" << std::endl;
        catalogue->wipeUnknown(unknownURIsCatalogue);
        for (const auto& [uri, storeState] : storeWipeStates) {
            const Store& store = storeState->store(config_);

            auto it = unknownURIsStore.find(uri);
            if (it == unknownURIsStore.end()) {
                store.doWipeUnknownContents(std::vector<eckit::URI>{});
            }
            else {
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

    eckit::Log::info() << "WipeCoordinator::wipe - completed" << std::endl;
}


}  // namespace fdb5