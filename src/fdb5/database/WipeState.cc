#include "fdb5/database/WipeState.h"
#include <memory>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

// -----------------------------------------------------------------------------------------------

WipeState::WipeState() {}  // rm these entirely?

WipeState::WipeState(eckit::Stream& s) {}

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
void WipeState::encode(eckit::Stream& s) const {}


void WipeState::lock() {
    locked_ = true;

    // Sanity check: ensure there is no overlap between the 3 sets.
    URIIterator deleteURIs = flattenedDeleteURIs();
    for (auto& uri : deleteURIs) {
        ASSERT(safeURIs_.find(uri) == safeURIs_.end());
    }

    for (auto& uri : unknownURIs_) {
        ASSERT(safeURIs_.find(uri) == safeURIs_.end());
    }

    for (auto& uri : deleteURIs) {
        ASSERT(unknownURIs_.find(uri) == unknownURIs_.end());
    }
}

URIIterator WipeState::flattenedDeleteURIs() const {
    return URIIterator{deleteURIs_};
}

// -----------------------------------------------------------------------------------------------

void CatalogueWipeState::includeData(const eckit::URI& dataURI) {
    // We should make these add directory to the store state, imo. Though maybe it'll need some uri scheme faffing

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
}

void CatalogueWipeState::encode(eckit::Stream& s) const {
    WipeState::encode(s);
    s << dbKey_;

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

void CatalogueWipeState::signStoreStates(std::string secret) {
    for (auto& [uri, state] : storeWipeStates_) {
        state->sign(secret);
    }
}

// This is backend specific: should be in toccatalogue class.
// XXX -- check where this is used. Maybe it is not strictly required.
bool CatalogueWipeState::ownsURI(const eckit::URI& uri_in) const {

    // We need to reconcile these URIs with the "include URIs", which come from the catalogue I think.
    // Maybe there isn't a reason to keep these in separate sets.

    if (!(uri_in.scheme() == "file" || uri_in.scheme() == "toc")) {
        return false;
    }

    eckit::URI astoc  = eckit::URI("toc", uri_in.path());
    eckit::URI asfile = eckit::URI("file", uri_in.path());

    // XXX - hack. because we seemingly interchangeably use file and toc schemes
    // Correct behaviour would be to prevent those uris from getting this far.
    for (const auto& uri : {astoc, asfile}) {

        if (std::find(safeURIs().begin(), safeURIs().end(), uri) != safeURIs().end()) {
            return true;
        }

        auto deleteURIs = flattenedDeleteURIs();

        if (std::find(deleteURIs.begin(), deleteURIs.end(), uri) != deleteURIs.end()) {
            return true;
        }
    }

    return false;
}

// -----------------------------------------------------------------------------------------------

void StoreWipeState::sign(const std::string& secret) {
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

StoreWipeState::StoreWipeState(eckit::Stream& s) {

    auto signature = Signature(s);
    s >> storeURI_;

    std::size_t n = 0;

    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        includeData(uri);
    }

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

    s << static_cast<std::size_t>(includedDataURIs().size());
    for (const auto& uri : includedDataURIs()) {
        s << uri;
    }

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

    // store's safe uris?

    return wipeElements;
}

bool StoreWipeState::ownsURI(const eckit::URI& uri) const {

    auto dataURIs = flattenedDeleteURIs();
    if (std::find(dataURIs.begin(), dataURIs.end(), uri) != dataURIs.end()) {
        return true;
    }

    return false;
}

// -----------------------------------------------------------------------------------------------

}  // namespace fdb5