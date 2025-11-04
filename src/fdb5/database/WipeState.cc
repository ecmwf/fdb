#include "fdb5/database/WipeState.h"
#include <memory>
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

// -----------------------------------------------------------------------------------------------

WipeState::WipeState(const Key& dbKey) : dbKey_(dbKey) {}

WipeState::WipeState(const Key& dbKey, WipeElements elements) : wipeElements_(std::move(elements)), dbKey_(dbKey) {}

WipeState::WipeState(eckit::Stream& s) {
    // signature_ = Signature(s) is equivalent to:
    signature_ = Signature(s);

    s >> dbKey_;

    std::size_t n = 0;

    // wipeElements_
    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        wipeElements_.emplace_back(std::make_shared<WipeElement>(s));
    }

    // includeDataURIs_
    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        includeDataURIs_.insert(uri);
    }

    // excludeDataURIs_
    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        excludeDataURIs_.insert(uri);
    }
}

std::size_t WipeState::encodeSize() const {
    // approximate... do this better...
    std::size_t size = 0;
    size += sizeof(std::size_t);  // key string size (placeholder/approx)
    size += 256;                  // dbkey string (approx)
    size += sizeof(std::size_t);  // number of wipe elements
    for (const auto& el : wipeElements_) {
        size += el->encodeSize();
    }
    size += 2 * sizeof(std::size_t);  // num include + num exclude
    size += (includeDataURIs_.size() + excludeDataURIs_.size()) * 256;
    return size;
}

void WipeState::include(const eckit::URI& uri) {
    failIfSigned();
    includeDataURIs_.insert(uri);
}

void WipeState::exclude(const eckit::URI& uri) {
    failIfSigned();
    excludeDataURIs_.insert(uri);
}

void WipeState::encode(eckit::Stream& s) const {
    // Encoding implies we are sending to the Store/Catalogue for wiping, which requires signing.
    if (!signature_.isSigned()) {
        throw eckit::SeriousBug("WipeState must be signed before encoding");
    }

    s << signature_;
    s << dbKey_;

    s << static_cast<std::size_t>(wipeElements_.size());
    for (const auto& el : wipeElements_) {
        s << *el;
    }

    s << static_cast<std::size_t>(includeDataURIs_.size());
    for (const auto& uri : includeDataURIs_) {
        s << uri;
    }

    s << static_cast<std::size_t>(excludeDataURIs_.size());
    for (const auto& uri : excludeDataURIs_) {
        s << uri;
    }
}

void WipeState::print(std::ostream& out) const {
    out << "WipeState(dbKey=" << dbKey_.valuesToString() << ", wipeElements=[";
    std::string sep;

    sep.clear();
    for (const auto& el : wipeElements_) {
        out << sep << *el;
        sep = ",";
    }

    out << "], includeDataURIs=[";
    sep.clear();
    for (const auto& uri : includeDataURIs_) {
        out << sep << uri;
        sep = ",";
    }

    out << "], excludeDataURIs=[";
    sep.clear();
    for (const auto& uri : excludeDataURIs_) {
        out << sep << uri;
        sep = ",";
    }
    out << "])";
}

void WipeState::sign(std::string secret) {
    signature_.sign(hash(std::move(secret)));
}

std::uint64_t WipeState::hash(std::string secret) const {
    std::uint64_t h = Signature::hashURIs(includeDataURIs_, secret + "in");
    h ^= Signature::hashURIs(excludeDataURIs_, secret + "ex");
    return h;
}

void WipeState::failIfSigned() const {
    if (signature_.isSigned()) {
        throw eckit::SeriousBug("Attempt to modify signed WipeState");
    }
}

// -----------------------------------------------------------------------------------------------

StoreWipeState::StoreWipeState(eckit::URI uri) : WipeState(Key()), storeURI_(std::move(uri)) {}

StoreWipeState::StoreWipeState(eckit::Stream& s) : WipeState(s) {
    s >> storeURI_;
}

Store& StoreWipeState::store(const Config& config) const {
    if (!store_) {
        store_ = StoreFactory::instance().build(storeURI_, config);
    }
    return *store_;
}

void StoreWipeState::encode(eckit::Stream& s) const {
    WipeState::encode(s);
    s << storeURI_;
}

// -----------------------------------------------------------------------------------------------

CatalogueWipeState::CatalogueWipeState(const Key& dbKey) : WipeState(dbKey) {}

CatalogueWipeState::CatalogueWipeState(const Key& dbKey, WipeElements elements) :
    WipeState(dbKey, std::move(elements)) {}

CatalogueWipeState::CatalogueWipeState(eckit::Stream& s) : WipeState(s) {
    size_t n;
    s >> n;

    for (size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        auto state = std::make_unique<StoreWipeState>(s);
        storeWipeStates_.emplace(uri, std::move(state));
    }
}

void CatalogueWipeState::encode(eckit::Stream& s) const {
    WipeState::encode(s);

    s << static_cast<size_t>(storeWipeStates_.size());
    for (const auto& [uri, state] : storeWipeStates_) {
        s << uri;
        s << *state;
    }
}

eckit::Stream& operator<<(eckit::Stream& s, const CatalogueWipeState& state) {
    state.encode(s);
    return s;
}

const std::vector<Index>& CatalogueWipeState::indexesToMask() const {
    return indexesToMask_;
}

void CatalogueWipeState::buildStoreStates() {
    ASSERT(storeWipeStates_.empty());

    auto storeState = [&](const eckit::URI& storeURI) -> StoreWipeState& {
        auto [it, inserted] = storeWipeStates_.try_emplace(storeURI, nullptr);
        if (inserted || !it->second) {
            it->second = std::make_unique<StoreWipeState>(it->first);
        }
        return *it->second;
    };

    for (const auto& dataURI : includeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).include(dataURI);
    }

    for (const auto& dataURI : excludeURIs()) {
        eckit::URI storeURI = StoreFactory::instance().uri(dataURI);
        storeState(storeURI).exclude(dataURI);
    }
}

CatalogueWipeState::StoreStates CatalogueWipeState::takeStoreStates() const {
    return std::exchange(storeWipeStates_, {});
}

CatalogueWipeState::StoreStates& CatalogueWipeState::storeStates() {
    return storeWipeStates_;
}

void CatalogueWipeState::signStoreStates(std::string secret) {
    std::cout << "ZZZ Signing CatalogueWipeState store states" << std::endl;
    for (auto& [uri, state] : storeWipeStates_) {
        state->sign(secret);
        std::cout << "ZZZ StoreWipeState for " << uri << " signature: " << state->signature().sig_ << std::endl;
    }

    sign(secret);

    std::cout << "ZZZ Catalogue signature: " << signature_.sig_ << std::endl;
}
// ----

// todo: break this logic up a bit more, for readability.
// Almost certainly std::unique_ptr<CatalogueWipeState> can just be CatalogueWipeState everywhere...
std::unique_ptr<CatalogueWipeState> WipeCoordinator::wipe(const CatalogueWipeState& catalogueWipeState, bool doit,
                                                          bool unsafeWipeAll) const {

    bool error   = false;                                         ///
    bool canWipe = true;                                          /// !!! unused.
    bool wipeAll = true;                                          /// ?
    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;  /// < URIs not belonging to any store or catalogue.
    std::map<WipeElementType, std::shared_ptr<WipeElement>>
        aggregateStoreElements;  /// < Aggregate of all Wipe Elements from all stores

    auto report = std::make_unique<CatalogueWipeState>(catalogueWipeState.dbKey());

    // Establish whether we will fully wipe the catalogue, and gather unknown URIs if so.
    for (const auto& el : catalogueWipeState.wipeElements()) {
        if (el->type() == WipeElementType::WIPE_CATALOGUE_SAFE && !el->uris().empty()) {
            wipeAll = false;  // but I would assume wiping all on the catalogue ought to be decoupled from wiping all on
                              // the store.
            break;
        }

        // if wipeAll, collect all the unknown URIs
        if (wipeAll && el->type() == WipeElementType::WIPE_UNKNOWN && !el->uris().empty()) {
            for (const auto& uri : el->uris()) {
                unknownURIs.emplace(uri, std::nullopt);
            }
        }
    }

    eckit::Log::info() << "WipeCoordinator::wipe - gathering store wipe states" << std::endl;
    auto storeWipeStates = catalogueWipeState.takeStoreStates();

    ASSERT(!storeWipeStates.empty());  // XXX: We should handle case where there is no work for the stores...

    eckit::Log::info() << "WipeCoordinator::wipe - processing store wipe states" << std::endl;

    // Contact each of the relevant stores.
    for (const auto& [storeURI, storeStatePtr] : storeWipeStates) {
        auto& storeState = *storeStatePtr;

        if (!storeState.excludeURIs().empty()) {
            wipeAll = false;  // But I would assume this should be on a per store basis...
        }

        storeState.store(config_).prepareWipe(storeState, wipeAll);

        for (const auto& el : storeState.wipeElements()) {
            const auto type = el->type();

            // If wipeAll, collect all the unknown URIs
            if (wipeAll && type == WipeElementType::WIPE_UNKNOWN) {
                for (const auto& uri : el->uris()) {
                    unknownURIs.try_emplace(uri, storeURI);
                }
                continue;
            }

            // Group elements by type, merging URIs if the type already exists
            auto [it, inserted] = aggregateStoreElements.emplace(type, el);
            if (!inserted) {
                auto& target = *it->second;
                for (const auto& uri : el->uris()) {
                    target.add(uri);
                }
            }
        }
    }

    // Helpers ...
    auto isCatalogueElement = [](const auto& el) {
        const auto t = el->type();
        return (t == WipeElementType::WIPE_CATALOGUE || t == WipeElementType::WIPE_CATALOGUE_AUX) &&
               !el->uris().empty();
    };

    auto isStoreElement = [](const auto& el) {
        const auto t = el->type();
        return t == WipeElementType::WIPE_STORE || t == WipeElementType::WIPE_STORE_AUX;
    };


    // Check if unknowns are owned by the catalogue / stores.

    ASSERT(aggregateStoreElements.size() != 0);
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

            for (const auto& [type, el] : aggregateStoreElements) {
                if (isStoreElement(el) && el->uris().count(uri)) {
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
        auto el = std::make_shared<WipeElement>(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
        report->insertWipeElement(el);
        error = true;
    }

    // gather all non-unknown elements from the catalogue and the stores
    for (const auto& el : catalogueWipeState.wipeElements()) {
        if (el->type() != WipeElementType::WIPE_UNKNOWN) {
            report->insertWipeElement(el);
        }
    }

    for (const auto& [type, el] : aggregateStoreElements) {
        if (type != WipeElementType::WIPE_UNKNOWN) {
            report->insertWipeElement(el);
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

    auto el = std::make_shared<WipeElement>(WipeElementType::WIPE_UNKNOWN,
                                            "Unexpected entries in FDB database:", std::move(unknownURIsSet));
    report->insertWipeElement(el);

    // todo: this should probably be reported via wipe element
    const auto& indexesToMask = catalogueWipeState.indexesToMask();
    if (!indexesToMask.empty()) {
        eckit::Log::info() << "Masking indexes: " << std::endl;
        for (const auto& index : indexesToMask) {
            eckit::Log::info() << index << std::endl;  // !! issues on remote fdb?
        }
    }

    if (error && doit) {
        ASSERT(false);  /// xxx:: actual error
        return report;
    }

    if (doit && !error) {
        eckit::Log::info() << "WipeCoordinator::wipe - DOIT! performing wipe operations" << std::endl;
        // We shall do the following, in order:
        // - 1. mask any indexes that need masking.
        // - 2. wipe the unknown files in catalogue and stores.
        // - 3. wipe the files known by the stores.
        // - 4. wipe the files known by the catalogue.
        // - 5. wipe empty databases.

        std::unique_ptr<Catalogue> catalogue =
            CatalogueReaderFactory::instance().build(catalogueWipeState.dbKey(), config_);


        // 1. Mask indexes TODO
        // eckit::Log::info() << "WipeCoordinator::wipe - masking indexes" << std::endl;
        // for (const auto& index : catalogueWipeState.indexesToMask()) {
        //     catalogue->maskIndexEntry(index); // This is way too chatty for remote fdb. This should be one function
        //     call. (Also, handler knows what indexes to match, probably, unless there's been an update.)
        // }

        // 2. Wipe unknown files
        eckit::Log::info() << "WipeCoordinator::wipe - wiping unknown URIs"
                           << std::endl;  // THE REST OF THE LOGIC ASSUMES WE ONLY DO THIS ON WIPE ALL!!!!!
        catalogue->wipeUnknown(unknownURIsCatalogue);

        for (const auto& [uri, storeState] : storeWipeStates) {
            const Store& store = storeState->store(config_);

            // should this only be for wipe all? I think maybe...
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

    return report;
}


}  // namespace fdb5