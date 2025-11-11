#include "fdb5/database/WipeState.h"
#include <memory>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb5 {

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

WipeElements CatalogueWipeState::generateWipeElements() const {
    ASSERT(wipeElements_.empty());  // XXX todo: remove wipeElements_ from class.

    WipeElements wipeElements;
    if (!info_.empty()) {
        wipeElements.emplace_back(
            std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_INFO, info_,
                                          std::set<eckit::URI>{}));  // The empty set here is stupid. Give the element a
                                                                     // constructor that doesnt need a set...
    }

    if (!catalogueURIs_.empty()) {
        auto catalogueURIs = catalogueURIs_;
        wipeElements.emplace_back(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                                                                "Catalogue URIs to delete:", std::move(catalogueURIs)));
    }
    if (auxCatalogueURIs_.empty()) {
        auto auxURIs = auxCatalogueURIs_;
        wipeElements.emplace_back(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE_CONTROL,
                                                                "Control URIs to delete:", std::move(auxURIs)));
    }

    if (!safeURIs().empty()) {
        auto safe = safeURIs();
        wipeElements.emplace_back(std::make_shared<WipeElement>(
            WipeElementType::WIPE_CATALOGUE_SAFE, "Protected URIs (explicitly untouched):", std::move(safe)));
    }

    if (!indexURIs_.empty()) {
        auto indexURIs = indexURIs_;
        wipeElements.emplace_back(std::make_shared<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                                                                "Index URIs to delete:", std::move(indexURIs)));
    }

    return wipeElements;
}

WipeElements StoreWipeState::generateWipeElements() const {
    ASSERT(wipeElements_.empty());  // XXX todo: remove wipeElements_ from class.

    WipeElements wipeElements;

    if (!dataURIs_.empty()) {
        auto dataURIs = dataURIs_;
        wipeElements.emplace_back(
            std::make_shared<WipeElement>(WipeElementType::WIPE_STORE, "Data URIs to delete:", std::move(dataURIs)));
    }

    if (!auxURIs_.empty()) {
        auto auxURIs = auxURIs_;
        wipeElements.emplace_back(std::make_shared<WipeElement>(WipeElementType::WIPE_STORE_AUX,
                                                                "Auxiliary URIs to delete:", std::move(auxURIs)));
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