#include "fdb5/database/WipeState.h"
#include <memory>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb5 {

// -----------------------------------------------------------------------------------------------

WipeState::WipeState() {}


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

void CatalogueWipeState::includeData(const eckit::URI& uri) {
    // We should make these add directory to the store state, imo. Though maybe it'll need some uri scheme faffing

    eckit::URI storeURI = StoreFactory::instance().uri(uri);

    auto [it, inserted] = storeWipeStates_.try_emplace(storeURI, nullptr);
    if (inserted || !it->second) {
        it->second = std::make_unique<StoreWipeState>(it->first);
    }

    it->second->includeData(uri);
}

void CatalogueWipeState::excludeData(const eckit::URI& uri) {

    eckit::URI storeURI = StoreFactory::instance().uri(uri);

    auto [it, inserted] = storeWipeStates_.try_emplace(storeURI, nullptr);
    if (inserted || !it->second) {
        it->second = std::make_unique<StoreWipeState>(it->first);
    }

    it->second->excludeData(uri);
}

void WipeState::encode(eckit::Stream& s) const {}

void StoreWipeState::sign(const std::string& secret) {
    signature_.sign(hash(secret));
}

std::uint64_t StoreWipeState::hash(const std::string& secret) const {
    std::uint64_t h = Signature::hashURIs(includeDataURIs_, secret + "in");
    h ^= Signature::hashURIs(excludeDataURIs_, secret + "ex");
    return h;
}

void StoreWipeState::failIfSigned() const {
    if (signature_.isSigned()) {
        throw eckit::SeriousBug("Attempt to modify signed WipeState");
    }
}

// -----------------------------------------------------------------------------------------------

StoreWipeState::StoreWipeState(eckit::URI uri) : WipeState(), storeURI_(std::move(uri)) {}

StoreWipeState::StoreWipeState(eckit::Stream& s) : WipeState(s) {

    signature_ = Signature(s);
    s >> storeURI_;

    std::size_t n = 0;

    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        includeDataURIs_.insert(uri);
    }

    s >> n;
    for (std::size_t i = 0; i < n; ++i) {
        eckit::URI uri(s);
        excludeDataURIs_.insert(uri);
    }
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

    s << static_cast<std::size_t>(includeDataURIs_.size());
    for (const auto& uri : includeDataURIs_) {
        s << uri;
    }

    s << static_cast<std::size_t>(excludeDataURIs_.size());
    for (const auto& uri : excludeDataURIs_) {
        s << uri;
    }
}

WipeElements CatalogueWipeState::generateWipeElements() const {

    WipeElements wipeElements;
    if (!info_.empty()) {
        wipeElements.emplace_back(WipeElementType::WIPE_CATALOGUE_INFO, info_,
                                  std::set<eckit::URI>{});  // The empty set here is stupid. Give the element a
                                                            // constructor that doesnt need a set...
    }

    if (!catalogueURIs_.empty()) {
        auto catalogueURIs = catalogueURIs_;
        wipeElements.emplace_back(WipeElementType::WIPE_CATALOGUE,
                                  "Catalogue URIs to delete:", std::move(catalogueURIs));
    }
    if (auxCatalogueURIs_.empty()) {
        auto auxURIs = auxCatalogueURIs_;
        wipeElements.emplace_back(WipeElementType::WIPE_CATALOGUE_CONTROL,
                                  "Control URIs to delete:", std::move(auxURIs));
    }

    if (!safeURIs().empty()) {
        auto safe = safeURIs();
        wipeElements.emplace_back(WipeElementType::WIPE_CATALOGUE_SAFE,
                                  "Protected URIs (explicitly untouched):", std::move(safe));
    }

    if (!indexURIs_.empty()) {
        auto indexURIs = indexURIs_;
        wipeElements.emplace_back(WipeElementType::WIPE_CATALOGUE, "Index URIs to delete:", std::move(indexURIs));
    }

    return wipeElements;
}

WipeElements StoreWipeState::generateWipeElements() const {

    WipeElements wipeElements;

    if (!dataURIs_.empty()) {
        auto dataURIs = dataURIs_;
        wipeElements.emplace_back(WipeElementType::WIPE_STORE, "Data URIs to delete:", std::move(dataURIs));
    }

    if (!auxURIs_.empty()) {
        auto auxURIs = auxURIs_;
        wipeElements.emplace_back(WipeElementType::WIPE_STORE_AUX, "Auxiliary URIs to delete:", std::move(auxURIs));
    }

    return wipeElements;
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

CatalogueWipeState::StoreStates CatalogueWipeState::takeStoreStates() const {
    return std::exchange(storeWipeStates_, {});
}

CatalogueWipeState::StoreStates& CatalogueWipeState::storeStates() {
    return storeWipeStates_;
}

void CatalogueWipeState::signStoreStates(std::string secret) {
    for (auto& [uri, state] : storeWipeStates_) {
        state->sign(secret);
    }
}

// This is backend specific: should be in toccatalogue class.
bool CatalogueWipeState::ownsURI(const eckit::URI& uri_in) const {

    // We need to reconcile these URIs with the "include URIs", which come from the catalogue I think.
    // Maybe there isn't a reason to keep these in separate sets.

    if (!(uri_in.scheme() == "file" || uri_in.scheme() == "toc")) {
        return false;
    }

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
    }

    return false;
}


}  // namespace fdb5