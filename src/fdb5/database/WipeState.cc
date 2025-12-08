#include "fdb5/database/WipeState.h"
#include <memory>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

// -----------------------------------------------------------------------------------------------

WipeState::WipeState() {}

WipeState::WipeState(eckit::Stream& s) {

    // deleteURIs_
    size_t n;
    s >> n;
    for (size_t i = 0; i < n; ++i) {
        int typeInt;
        s >> typeInt;
        WipeElementType type = static_cast<WipeElementType>(typeInt);

        size_t uriCount;
        s >> uriCount;
        std::set<eckit::URI> uris;
        for (size_t j = 0; j < uriCount; ++j) {
            eckit::URI uri(s);
            uris.insert(uri);
        }
        deleteURIs_.emplace(type, std::move(uris));
    }

    // safeURIs_
    s >> n;
    for (size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        safeURIs_.insert(uri);
    }

    // unknownURIs_
    s >> n;
    for (size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        unknownURIs_.insert(uri);
    }
}

/// @todo: implement properly
std::size_t WipeState::encodeSize() const {
    // approximate... do this better...
    std::size_t size = 0;
    size += sizeof(std::size_t);      // key string size
    size += 256;                      // dbkey string
    size += sizeof(std::size_t);      // number of wipe elements
    size += 2 * sizeof(std::size_t);  // num include + num exclude
    // size += (includeDataURIs_.size() + excludeDataURIs_.size()) * 256;
    return size;
}

void WipeState::encode(eckit::Stream& s) const {

    // deleteURIs_
    s << deleteURIs_.size();
    for (const auto& [type, uris] : deleteURIs_) {
        s << static_cast<int>(type);
        s << static_cast<std::size_t>(uris.size());
        for (const auto& uri : uris) {
            s << uri;
        }
    }

    // safeURIs_
    s << safeURIs_.size();
    for (const auto& uri : safeURIs_) {
        s << uri;
    }

    // unknownURIs_
    s << unknownURIs_.size();
    for (const auto& uri : unknownURIs_) {
        s << uri;
    }
}

bool WipeState::isMarkedForDeletion(const eckit::URI& uri) const {
    for (const auto& [type, uris] : deleteURIs_) {
        if (uris.find(uri) != uris.end()) {
            return true;
        }
    }
    return false;
}

bool WipeState::isMarkedSafe(const eckit::URI& uri) const {
    return safeURIs_.find(uri) != safeURIs_.end();
}

void WipeState::sanityCheck() const {

    for (auto& uri : unknownURIs_) {
        ASSERT(!isMarkedSafe(uri));
        ASSERT(!isMarkedForDeletion(uri));
    }

    for (auto& uri : safeURIs_) {
        ASSERT(!isMarkedForDeletion(uri));
    }
}

// -----------------------------------------------------------------------------------------------

void CatalogueWipeState::includeData(const eckit::URI& dataURI) {

    eckit::URI storeURI = StoreFactory::instance().uri(dataURI);

    auto [it, inserted] = storeWipeStates_.try_emplace(storeURI, nullptr);
    if (inserted || !it->second) {
        it->second = std::make_unique<StoreWipeState>(it->first);
    }

    it->second->includeData(dataURI);
}

void CatalogueWipeState::excludeData(const eckit::URI& dataURI) {

    eckit::URI storeURI = StoreFactory::instance().uri(dataURI);

    auto [it, inserted] = storeWipeStates_.try_emplace(storeURI, nullptr);
    if (inserted || !it->second) {
        it->second = std::make_unique<StoreWipeState>(it->first);
    }

    it->second->excludeData(dataURI);
}

WipeElements CatalogueWipeState::extractWipeElements() {

    WipeElements wipeElements;
    if (!info_.empty()) {
        wipeElements.emplace_back(WipeElementType::CATALOGUE_INFO, info_);
    }

    auto addWipeElement = [&](WipeElementType type, const char* msg) {
        auto it = deleteURIs_.find(type);
        if (it == deleteURIs_.end() || it->second.empty()) {
            return;
        }

        wipeElements.emplace_back(type, msg, std::move(it->second));
        deleteURIs_.erase(it);
    };

    addWipeElement(WipeElementType::CATALOGUE, "Catalogue URIs to delete:");
    addWipeElement(WipeElementType::CATALOGUE_CONTROL, "Control URIs to delete:");
    addWipeElement(WipeElementType::CATALOGUE_INDEX, "Index URIs to delete:");

    if (!safeURIs().empty()) {
        auto safe = safeURIs();  // note: needless copy.
        wipeElements.emplace_back(WipeElementType::CATALOGUE_SAFE,
                                  "Protected URIs (explicitly untouched):", std::move(safe));
    }

    return wipeElements;
}

CatalogueWipeState::CatalogueWipeState(eckit::Stream& s) : WipeState(s) {

    s >> dbKey_;

    size_t n;
    s >> n;

    for (size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        auto state = std::make_unique<StoreWipeState>(s);
        storeWipeStates_.emplace(uri, std::move(state));
    }

    s >> info_;
}

void CatalogueWipeState::encode(eckit::Stream& s) const {
    WipeState::encode(s);
    s << dbKey_;

    s << static_cast<size_t>(storeWipeStates_.size());
    for (const auto& [uri, state] : storeWipeStates_) {
        s << uri;
        s << *state;
    }

    s << info_;
}

eckit::Stream& operator<<(eckit::Stream& s, const CatalogueWipeState& state) {
    state.encode(s);
    return s;
}

const std::set<Index>& CatalogueWipeState::indexesToMask() const {
    return indexesToMask_;
}

void CatalogueWipeState::signStoreStates(std::string secret) const {
    for (auto& [uri, state] : storeWipeStates_) {
        state->sign(secret);
    }
}

// -----------------------------------------------------------------------------------------------

void StoreWipeState::sign(const std::string& secret) const {
    signature_.sign(hash(secret));
}

std::uint64_t StoreWipeState::hash(const std::string& secret) const {
    std::uint64_t h = Signature::hashURIs(includedDataURIs(), secret + "in");
    h ^= Signature::hashURIs(excludeDataURIs_, secret + "ex");
    return h;
}

void StoreWipeState::failIfSigned() const {
    if (signature_.isSigned()) {
        throw eckit::SeriousBug("Attempt to modify signed WipeState");
    }
}

StoreWipeState::StoreWipeState(eckit::URI uri) : storeURI_(std::move(uri)) {}

StoreWipeState::StoreWipeState(eckit::Stream& s) : WipeState(s) {

    auto signature = Signature(s);
    s >> storeURI_;

    std::size_t n = 0;

    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        excludeDataURIs_.insert(uri);
    }

    signature_ = signature;
}

Store& StoreWipeState::store(const Config& config) const {
    if (!store_) {
        store_ = StoreFactory::instance().build(storeURI_, config);
    }
    return *store_;
}

void StoreWipeState::encode(eckit::Stream& s) const {

    if (!signature_.isSigned()) {
        throw eckit::SeriousBug("StoreWipeState must be signed before encoding");
    }

    WipeState::encode(s);

    s << signature_;
    s << storeURI_;


    s << static_cast<std::size_t>(excludeDataURIs_.size());
    for (const auto& uri : excludeDataURIs_) {
        s << uri;
    }
}

WipeElements StoreWipeState::extractWipeElements() {
    WipeElements wipeElements;

    auto addWipeElement = [&](WipeElementType type, const char* msg) {
        auto it = deleteURIs_.find(type);
        if (it == deleteURIs_.end() || it->second.empty()) {
            return;
        }

        wipeElements.emplace_back(type, msg, std::move(it->second));
        deleteURIs_.erase(it);
    };

    addWipeElement(WipeElementType::STORE, "Data URIs to delete:");
    addWipeElement(WipeElementType::STORE_AUX, "Auxiliary URIs to delete:");

    return wipeElements;
}

// -----------------------------------------------------------------------------------------------

}  // namespace fdb5