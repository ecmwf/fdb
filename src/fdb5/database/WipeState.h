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
#include <string>
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"


namespace fdb5 {

class Catalogue;
class Index;

class CatalogueWipeState;
using WipeStateIterator = APIIterator<std::unique_ptr<CatalogueWipeState>>;
using URIMap            = std::map<WipeElementType, std::set<eckit::URI>>;

class WipeState;

// Dummy placeholder for signed behaviour
class Signature {

public:

    static uint64_t hashURIs(const std::set<eckit::URI>& uris, const std::string& secret) {
        uint64_t h = 0;
        for (const auto& uri : uris) {
            h ^= std::hash<std::string>{}(uri.asRawString()) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        h ^= std::hash<std::string>{}(secret) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }

    Signature() {}

    Signature(eckit::Stream& s) { s >> sig_; }

    void sign(uint64_t sig) {
        ASSERT(sig != 0);
        sig_ = sig;
    }

    bool isSigned() const { return sig_ != 0; }

    friend eckit::Stream& operator<<(eckit::Stream& s, const Signature& sig) {
        s << sig.sig_;
        return s;
    }

    bool validSignature(uint64_t expected) const { return sig_ == expected; }

public:

    uint64_t sig_{0};
};

// Iterator that treats a map as a flattened object.
// I believe we could remove this in C++ 20 using Ranges.
struct URIIterator {
    using OuterIt = URIMap::const_iterator;
    using InnerIt = std::set<eckit::URI>::const_iterator;

    const URIMap& data_;

    struct iterator {
        OuterIt outer_;
        OuterIt outerEnd_;
        InnerIt inner_;

        iterator(OuterIt o, OuterIt oEnd) : outer_(o), outerEnd_(oEnd) {
            if (outer_ != outerEnd_) {
                inner_ = outer_->second.begin();
                skipEmptySets();
            }
        }

        // Advance to next valid element or to end
        void skipEmptySets() {
            while (outer_ != outerEnd_ && inner_ == outer_->second.end()) {
                ++outer_;
                if (outer_ != outerEnd_) {
                    inner_ = outer_->second.begin();
                }
            }
        }

        const eckit::URI& operator*() const { return *inner_; }

        iterator& operator++() {
            ++inner_;
            skipEmptySets();
            return *this;
        }

        bool operator==(const iterator& other) const {
            // When both are at end, outer == outerEnd in both
            if (outer_ == outerEnd_ && other.outer_ == other.outerEnd_) {
                return true;
            }
            return outer_ == other.outer_ && inner_ == other.inner_;
        }

        bool operator!=(const iterator& other) const { return !(*this == other); }
    };

    iterator begin() const { return iterator{data_.begin(), data_.end()}; }

    iterator end() const { return iterator{data_.end(), data_.end()}; }
};


class WipeState {
public:

    WipeState();

    WipeState(std::set<eckit::URI> safeURIs, URIMap deleteURIs) {
        safeURIs_   = std::move(safeURIs);
        deleteURIs_ = std::move(deleteURIs);
    }

    explicit WipeState(eckit::Stream& s);

    std::size_t encodeSize() const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state);

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

    // Create WipeElements from this Class's contents.
    // Note: this moves the data out of this class and into the WipeElements. The class is not intended to be used after
    // this function call. virtual WipeElements extractWipeElements() && = 0; // <-- This might make it more explicit
    // that the object will be left in a special state.
    virtual WipeElements extractWipeElements() = 0;

    // encode / decode
    void encode(eckit::Stream& s) const;

    const std::set<eckit::URI>& unrecognisedURIs() const { return unknownURIs_; }
    void insertUnrecognised(const eckit::URI& uri) { unknownURIs_.insert(uri); }

    virtual bool wipeAll() const {
        return safeURIs_.empty();  // not really sufficient for a store
    }

    // No more uris may be marked as safe or for deletion.
    void lock() {
        locked_ = true;

        // Sanity check: ensure there is no overlap between the 3 sets.
        URIIterator deleteURIs = FlattenedDeleteURIs();
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


    const std::set<eckit::URI>& safeURIs() const { return safeURIs_; }

    const URIMap& deleteMap() const { return deleteURIs_; }

    URIIterator FlattenedDeleteURIs() const { return URIIterator{deleteURIs_}; }

    bool isNotMarkedAsSafe(const eckit::URI& uri) const { return safeURIs().find(uri) == safeURIs().end(); }

    void markAsSafe(const std::set<eckit::URI>& uris) {
        ASSERT(!locked_);
        safeURIs_.insert(uris.begin(), uris.end());
    }

    void markForDeletion(WipeElementType type, const std::set<eckit::URI>& uris) {
        for (auto& uri : uris) {
            markForDeletion(type, uri);
        }
    }

    void markForDeletion(WipeElementType type, const eckit::URI& uri) {
        ASSERT(!locked_);
        deleteURIs_[type].insert(uri);
    }

protected:

    std::set<eckit::URI> unknownURIs_;
    mutable URIMap deleteURIs_;

private:

    std::set<eckit::URI> safeURIs_;  // files explicitly not to be deleted. // <-- I dont think the store has any reason
                                     // to mark anything as safe. Just delete and unrecognised.
    bool locked_ = false;
};

/* ------------------------------ StoreWipeState ------------------------------ */

class StoreWipeState : public WipeState {
public:

    explicit StoreWipeState(eckit::URI uri);
    explicit StoreWipeState(eckit::Stream& s);

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const StoreWipeState& state) {
        state.encode(s);
        return s;
    }
    // const std::set<eckit::URI>& dataURIs() const { return dataURIs_; }
    const std::set<eckit::URI>& dataURIs() const { return includeDataURIs_; }
    void insertDataURI(const eckit::URI& uri) { includeDataURIs_.insert(uri); }  // is this still used?

    const std::set<eckit::URI>& dataAuxiliaryURIs() const { return auxURIs_; }
    void insertAuxiliaryURI(const eckit::URI& uri) { auxURIs_.insert(uri); }

    bool ownsURI(const eckit::URI& uri) const;

    void includeData(const eckit::URI& uri) {
        failIfSigned();
        includeDataURIs_.insert(uri);
    }
    void excludeData(const eckit::URI& uri) {
        failIfSigned();
        excludeDataURIs_.insert(uri);
    }

    Store& store(const Config& config) const;
    WipeElements extractWipeElements();

    const eckit::URI& storeURI() const { return storeURI_; }
    std::set<eckit::URI>& includeDataURIs() { return includeDataURIs_; }
    std::set<eckit::URI>& excludeDataURIs() { return excludeDataURIs_; }
    void insertUnrecognised(const eckit::URI& uri) { unknownURIs_.insert(uri); }
    const std::set<eckit::URI>& unrecognisedURIs() const { return unknownURIs_; }

    // signing

    void sign(const std::string& secret);
    const Signature& signature() const { return signature_; }
    std::uint64_t hash(const std::string& secret) const;

private:

    void failIfSigned() const;

private:

    Signature signature_;

    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;  // We dont really use this?

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;

    std::set<eckit::URI> auxURIs_;
    std::set<eckit::URI> unknownURIs_;
};

class CatalogueWipeState : public WipeState {

    using StoreStates = std::map<eckit::URI, std::unique_ptr<StoreWipeState>>;

public:

    explicit CatalogueWipeState(const Key& dbKey) : WipeState(), dbKey_(dbKey) {}

    CatalogueWipeState(const Key& dbKey, std::set<eckit::URI> safeURIs, URIMap deleteURIs) :
        WipeState(std::move(safeURIs), std::move(deleteURIs)), dbKey_(dbKey) {}

    explicit CatalogueWipeState(eckit::Stream& s);

    virtual ~CatalogueWipeState() = default;

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const CatalogueWipeState& state);

    const std::vector<Index>& indexesToMask() const;

    StoreStates& storeStates() { return storeWipeStates_; }

    void signStoreStates(std::string secret);

    WipeElements extractWipeElements() override;

    bool ownsURI(const eckit::URI& uri) const;

    void includeData(const eckit::URI& uri);
    void excludeData(const eckit::URI& uri);

    const Key& dbKey() const { return dbKey_; }

    void markForMasking(Index idx) { indexesToMask_.push_back(idx); }

    void info(const std::string& in) { info_ = in; }

private:

    // For finding the catalogue again later.
    Key dbKey_;

    std::vector<Index> indexesToMask_ = {};  // @todo, use this...
    StoreStates storeWipeStates_;

    std::string info_;  // Additional info about this particular catalogue (e.g. owner)
};


// ------------------------------------------------------------------------------------------------------

}  // namespace fdb5