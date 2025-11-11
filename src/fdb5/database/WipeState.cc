#include "fdb5/database/WipeState.h"
#include <memory>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
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
    signature_ = Signature(s);

    s >> dbKey_;

    std::size_t n = 0;

    // wipeElements_ // TODO: I dont think there's a reason to communicate these ever.
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

void CatalogueWipeState::generateWipeElements() {
    ASSERT(wipeElements_.empty());

    if (!info_.empty()) {
        insertWipeElement(
            std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_INFO, info_,
                                          std::set<eckit::URI>{}));  // The empty set here is stupid. Give the element a
                                                                     // constructor that doesnt need a set...
    }

    if (!catalogueURIs_.empty()) {
        auto catalogueURIs = catalogueURIs_;
        insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                                                        "Catalogue URIs to delete:", std::move(catalogueURIs)));
    }
    if (auxCatalogueURIs_.empty()) {
        auto auxURIs = auxCatalogueURIs_;
        insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_CONTROL,
                                                        "Control URIs to delete:", std::move(auxURIs)));
    }

    if (!unrecognisedURIs().empty()) {
        std::set<eckit::URI> unrecognised = unrecognisedURIs();
        insertWipeElement(std::make_shared<WipeElement>(
            WipeElementType::WIPE_UNKNOWN, "Unexpected URIs present in the catalogue:", std::move(unrecognised)));
    }

    if (!safeURIs().empty()) {
        auto safe = safeURIs();
        insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_SAFE,
                                                        "Protected URIs (explicitly untouched):", std::move(safe)));
    }

    if (!indexURIs_.empty()) {
        auto indexURIs = indexURIs_;
        insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                                                        "Index URIs to delete:", std::move(indexURIs)));
    }

    // if (!unknownURIs_.empty()) {
    //     auto unknownURIs = unknownURIs_;
    //     wipeElements_.push_back(std::make_shared<WipeElement>(
    //         WipeElementType::WIPE_UNKNOWN, "Unexpected URIs present in store:", std::move(unknownURIs)));
    // }

    // if (!dataURIs_.empty()) {
    //     auto dataURIs = dataURIs_;
    //     wipeElements_.push_back(
    //         std::make_shared<WipeElement>(WipeElementType::WIPE_STORE, "Data URIs to delete:", std::move(dataURIs)));
    // }

    // if (!auxURIs_.empty()) {
    //     auto auxURIs = auxURIs_;
    //     wipeElements_.push_back(std::make_shared<WipeElement>(WipeElementType::WIPE_STORE_AUX,
    //                                                          "Auxiliary URIs to delete:", std::move(auxURIs)));
    // }
}

bool StoreWipeState::ownsURI(const eckit::URI& uri) const {

    // We need to reconcile these URIs with the "include URIs", which come from the catalogue I think.

    if (std::find(dataURIs_.begin(), dataURIs_.end(), uri) != dataURIs_.end()) {
        return true;
    }
    if (std::find(auxURIs_.begin(), auxURIs_.end(), uri) != auxURIs_.end()) {
        return true;
    }

    return false;
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


bool CatalogueWipeState::ownsURI(const eckit::URI& uri_in) const {

    // We need to reconcile these URIs with the "include URIs", which come from the catalogue I think.
    // Maybe there isn't a reason to keep these in separate sets.

    eckit::URI astoc  = eckit::URI("toc", uri_in.path());
    eckit::URI asfile = eckit::URI("file", uri_in.path());

    // XXX - hack. because we seemingly interchangeably use file and toc schemes
    for (const auto& uri : {astoc, asfile}) {

        if (std::find(safeURIs().begin(), safeURIs().end(), uri) != safeURIs().end()) {
            return true;
        }

        if (std::find(deleteURIs().begin(), deleteURIs().end(), uri) != deleteURIs().end()) {
            return true;
        }

        // ideally, remove all of these below. I want these to be explicitly subset / buckets of safe / delete URIs.
        // if (std::find(indexURIs_.begin(), indexURIs_.end(), uri) != indexURIs_.end()) {
        //     return true;
        // }

        // if (std::find(catalogueURIs_.begin(), catalogueURIs_.end(), uri) != catalogueURIs_.end()) {
        //     return true;
        // }

        // if (std::find(auxCatalogueURIs_.begin(), auxCatalogueURIs_.end(), uri) != auxCatalogueURIs_.end()) {
        //     return true;
        // }
    }

    return false;
}

// ---------------------------------------------------------------------------------------------------

// todo: break this logic up a bit more, for readability.
// Almost certainly std::unique_ptr<CatalogueWipeState> can just be CatalogueWipeState everywhere...

void gatherUnknowns(const CatalogueWipeState& catalogueWipeState) {}

std::unique_ptr<CatalogueWipeState> WipeCoordinator::wipe(const CatalogueWipeState& catalogueWipeState, bool doit,
                                                          bool unsafeWipeAll) const {

    bool unclean = false;  ///
    bool canWipe = true;   /// !!! unused.

    std::map<eckit::URI, std::optional<eckit::URI>> unknownURIs;  /// < URIs not belonging to any store or catalogue.

    // ASSERT(catalogueWipeState.wipeElements().size() != 0);
    ASSERT(catalogueWipeState.wipeElements().size() ==
           0);  // I expect it to equal 0 because im deliberately not generating them. I want to not generate them in
                // the state classes where possible.


    // Establish whether we will fully wipe the catalogue, and gather unknown URIs if so.
    // if (catalogueWipeState.wipeAll()) {
    // We mustn't have any thing marked as safe if we are doing a wipe all.
    ASSERT(catalogueWipeState.safeURIs().size() == 0);

    for (const auto& uri : catalogueWipeState.unrecognisedURIs()) {
        std::cout << "XXX cat unknown - " << uri << std::endl;
        unknownURIs.emplace(uri, std::nullopt);
    }
    // }

    eckit::Log::info() << "WipeCoordinator::wipe - gathering store wipe states" << std::endl;
    auto storeWipeStates = catalogueWipeState.takeStoreStates();

    ASSERT(!storeWipeStates.empty());  // XXX: We should handle case where there is no work for the stores...

    eckit::Log::info() << "WipeCoordinator::wipe - processing store wipe states" << std::endl;

    // XXX -- Why does the store not have a concept of "SAFE" URIS?
    // Answer: it does. But we never push to it in the toc store it seems, or check it here...

    // Contact each of the relevant stores.
    for (const auto& [storeURI, storeState] : storeWipeStates) {

        storeState->store(config_).prepareWipe(*storeState);

        // gather the unknowns
        // if (storeState->wipeAll()){
        for (const auto& uri : storeState->unrecognisedURIs()) {
            std::cout << "XXX store unknown - " << uri << " from store " << storeURI << std::endl;
            unknownURIs.try_emplace(uri, storeURI);  // we shouldn't need to copy these. we can get them when needed
                                                     // from storeState->unrecognisedURIs ?
        }
        // }
    }

    // Check if unknowns are owned by the catalogue / stores.

    if (unknownURIs.size() > 0) {  // unknownURIs.size() > 0) implies one of the stores / cat has set wipeall
        std::cout << "XXX - there are unknown URIs to check ownership of " << std::endl;

        auto it = unknownURIs.begin();
        while (it != unknownURIs.end()) {

            const auto& uri = it->first;

            // Do not treat this uri as unknown if the catalogue has already marked it for deletion.
            std::cout << "unknowns: checking uri " << uri << std::endl;
            if (catalogueWipeState.ownsURI(uri)) {
                std::cout << "-> owned by cat " << uri << std::endl;
                it = unknownURIs.erase(it);
                continue;
            }

            // Do not treat this uri as unknown if store has already marked it for deletion.
            bool found = false;
            for (const auto& [storeURI, storeState] : storeWipeStates) {
                if (storeState->ownsURI(uri)) {
                    std::cout << "-> owned by store " << uri << std::endl;
                    it    = unknownURIs.erase(it);
                    found = true;
                    break;
                    // XXX - Doesn't this have implications for whether the store state should be still marked as
                    // WipeAll? Multiple stores + cat really muddies what "wipe all" should mean...
                }
            }

            if (!found) {
                std::cout << "-> owned by no one " << uri << std::endl;  // going to be an error unless unsafeWipeAll
                ++it;
            }
        }
    }


    // PUSHING TO THE REPORT QUEUE
    auto report = std::make_unique<CatalogueWipeState>(catalogueWipeState.dbKey());

    eckit::Log::info() << "WipeCoordinator::wipe - pushing wipe elements to report queue" << std::endl;
    if (doit && !unknownURIs.empty() && !unsafeWipeAll) {
        std::cout << "XXX: error - unknowns are " << std::endl;
        for (auto [u, opt] : unknownURIs) {
            std::cout << "   " << u << std::endl;
        }
        auto el = std::make_shared<WipeElement>(WipeElementType::WIPE_ERROR, "Cannot fully wipe unclean FDB database:");
        report->insertWipeElement(el);
        unclean = true;
    }

    // gather all non-unknown elements from the catalogue and the stores
    // catalogueWipeState.generateWipeElements();
    for (const auto& el : catalogueWipeState.wipeElements()) {
        if (el->type() != WipeElementType::WIPE_UNKNOWN) {
            report->insertWipeElement(el);
        }
    }

    for (const auto& [storeURI, storeState] : storeWipeStates) {

        // todo: since we create these here, remove the generating function.
        // If any of this needs to be backend specific (so far, only the case for the cat, we can have the states do the
        // insertion themselves.)
        auto unknownURIs =
            storeState
                ->unrecognisedURIs();  // either we are aggregating the unknowns , or we are not. make up your mind.
        if (!unknownURIs.empty()) {
            report->insertWipeElement(std::make_shared<WipeElement>(
                WipeElementType::WIPE_UNKNOWN, "Unexpected URIs present in store:", std::move(unknownURIs)));
        }

        auto dataURIs = storeState->dataURIs();
        if (!dataURIs.empty()) {
            report->insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_STORE,
                                                                    "Data URIs to delete:", std::move(dataURIs)));
        }

        auto auxURIs = storeState->dataAuxiliaryURIs();
        if (!auxURIs.empty()) {
            report->insertWipeElement(std::make_shared<WipeElement>(WipeElementType::WIPE_STORE_AUX,
                                                                    "Auxiliary URIs to delete:", std::move(auxURIs)));
        }
    }


    // gather the unknowns.
    // This can surely be done better...
    eckit::Log::info() << "WipeCoordinator::wipe - gathering unknown URIs" << std::endl;
    std::set<eckit::URI> unknownURIsSet;
    std::vector<eckit::URI> unknownURIsCatalogue;  // I think we ought to be using these in the first place...
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

    // I feel like we are reporting the unknowns in several places.
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

    if (doit && unclean) {
        throw eckit::Exception("Cannot fully wipe unclean FDB database");
    }

    // print all of unknownURIsSet
    std::cout << "XXX, unknown URIs to wipe: " << std::endl;
    for (const auto& uri : unknownURIsSet) {
        std::cout << "   " << uri << std::endl;
    }

    if (doit && !unclean) {  // I'd love to put this in a doit function, called from outside.
        eckit::Log::info() << "WipeCoordinator::wipe - DOIT! performing wipe operations" << std::endl;
        // We shall do the following, in order:
        // - 1. mask any indexes that need masking.
        // - 2. wipe the unknown files in catalogue and stores.
        // - 3. wipe the files known by the stores.
        // - 4. wipe the files known by the catalogue.
        // - 5. wipe empty databases.

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
        eckit::Log::info() << "WipeCoordinator::wipe - wiping unknown URIs"
                           << std::endl;

        //XXX: We probably shouldn't bother calling these functions if there are no unknown URIs.
        catalogue->wipeUnknown(unknownURIsCatalogue);

        for (const auto& [uri, storeState] : storeWipeStates) {
            const Store& store = storeState->store(config_);

            auto it = unknownURIsStore.find(uri);
            if (it == unknownURIsStore.end()) {
                store.doWipeUnknownContents(std::vector<eckit::URI>{});  // what is the point of this?
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