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
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"


namespace fdb5 {

class CatalogueWipeState;
using WipeStateIterator = APIIterator<std::unique_ptr<CatalogueWipeState>>;
using URIMap            = std::map<WipeElementType, std::set<eckit::URI>>;

class Signature;

// Dummy placeholder for signing. We can argue over what we want to do later.
// The Catalogue Server signs the wipe states before sending them to clients,
// which the client will forward to the stores. The stores will verify the signatures before proceeding with the wipe.
class Signature {

public:

    static uint64_t hashURIs(const std::set<eckit::URI>& uris, const std::string& secret) {
        uint64_t h = 0;
        std::vector<std::string> sortedURIs;
        for (const auto& uri : uris) {
            sortedURIs.push_back(uri.asRawString());
        }
        std::sort(sortedURIs.begin(), sortedURIs.end());

        for (const auto& uri : sortedURIs) {
            h ^= std::hash<std::string>{}(uri) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        h ^= std::hash<std::string>{}(secret) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }

    Signature() {}

    Signature(eckit::Stream& s) {
        unsigned long long in;
        s >> in;
        sig_ = in;
    }

    void sign(uint64_t sig) {
        ASSERT(sig != 0);
        sig_ = sig;
    }

    bool isSigned() const { return sig_ != 0; }

    friend eckit::Stream& operator<<(eckit::Stream& s, const Signature& sig) {
        // pending patch in eckit to support uint64_t directly
        s << static_cast<unsigned long long>(sig.sig_);
        return s;
    }

    bool validSignature(uint64_t expected) const { return sig_ == expected; }

public:

    uint64_t sig_{0};
};

// -----------------------------------------------------------------------------------------------
// Class for storing all URIs to be wiped.
class WipeState {
public:

    WipeState();

    WipeState(std::set<eckit::URI> safeURIs, URIMap deleteURIs) {
        safeURIs_   = std::move(safeURIs);
        deleteURIs_ = std::move(deleteURIs);
    }

    virtual ~WipeState() = default;

    explicit WipeState(eckit::Stream& s);

    virtual void encode(eckit::Stream& s) const;

    std::size_t encodeSize() const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

    // Insert URIs

    void insertUnrecognised(const eckit::URI& uri) { unknownURIs_.insert(uri); }

    bool isMarkedForDeletion(const eckit::URI& uri) const;
    bool isMarkedSafe(const eckit::URI& uri) const;


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

    virtual bool wipeAll() const {
        return safeURIs_.empty();  // not really sufficient for a store
    }

    // No more uris may be marked as safe or for deletion.
    void lock();

    // Getters
    const std::set<eckit::URI>& unrecognisedURIs() const { return unknownURIs_; }
    const std::set<eckit::URI>& safeURIs() const { return safeURIs_; }
    const URIMap& deleteMap() const { return deleteURIs_; }


    // Create WipeElements from this Class's contents.
    // Note: this moves the data out of this class and into the WipeElements. The class is not intended to be used after
    // this function call.

    // virtual WipeElements extractWipeElements() && = 0; // <-- This might make it more explicit
    // that the object will be left in a special state.
    virtual WipeElements extractWipeElements() = 0;

protected:

    mutable URIMap deleteURIs_;  // why mutable?

private:

    std::set<eckit::URI> unknownURIs_;

    std::set<eckit::URI> safeURIs_;  // files explicitly not to be deleted. // <-- I dont think the store has any reason
                                     // to mark anything as safe. Just delete and unrecognised.
    bool locked_ = false;
};

// -----------------------------------------------------------------------------------------------

class StoreWipeState : public WipeState {
public:

    explicit StoreWipeState(eckit::URI uri);
    explicit StoreWipeState(eckit::Stream& s);

    // Adding and Removing URIs

    void insertAuxiliaryURI(const eckit::URI& uri) {
        // note: intentionally do not check signed.
        markForDeletion(WipeElementType::STORE_AUX, uri);
    }

    void includeData(const eckit::URI& uri) {
        failIfSigned();
        markForDeletion(WipeElementType::STORE, uri);
    }
    void excludeData(const eckit::URI& uri) {
        failIfSigned();
        excludeDataURIs_.insert(uri);
    }

    // Remove URI from list of URIs to be deleted because, for example, it turns out not exist.
    void unincludeURI(const eckit::URI& uri) { deleteURIs_[WipeElementType::STORE].erase(uri); }

    // Signing

    void sign(const std::string& secret);
    std::uint64_t hash(const std::string& secret) const;

    // Getters

    Store& store(const Config& config) const;
    const eckit::URI& storeURI() const { return storeURI_; }
    const Signature& signature() const { return signature_; }
    const std::set<eckit::URI>& dataAuxiliaryURIs() const { return deleteURIs_[WipeElementType::STORE_AUX]; }
    const std::set<eckit::URI>& includedDataURIs() const { return deleteURIs_[WipeElementType::STORE]; }
    const std::set<eckit::URI>& excludedDataURIs() const { return excludeDataURIs_; }

    // Overrides
    void encode(eckit::Stream& s) const override;
    WipeElements extractWipeElements() override;

private:

    void failIfSigned() const;

private:

    Signature signature_;

    std::set<eckit::URI> excludeDataURIs_;  // We dont really use this?

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;
};

// -----------------------------------------------------------------------------------------------

class CatalogueWipeState : public WipeState {

    using StoreStates = std::map<eckit::URI, std::unique_ptr<StoreWipeState>>;

public:

    explicit CatalogueWipeState(const Key& dbKey) : WipeState(), dbKey_(dbKey) {}

    CatalogueWipeState(const Key& dbKey, std::set<eckit::URI> safeURIs, URIMap deleteURIs) :
        WipeState(std::move(safeURIs), std::move(deleteURIs)), dbKey_(dbKey) {}

    explicit CatalogueWipeState(eckit::Stream& s);

    Catalogue& catalogue(const Config& config) const {
        if (!catalogue_) {
            catalogue_ = CatalogueReaderFactory::instance().build(dbKey_, config);
        }
        return *catalogue_;
    }

    void initialControlState(const ControlIdentifiers& ids) { initialControlState_ = ids; }

    // Reset the control state (i.e. locks) of the catalogue to their pre-wipe state.
    void resetControlState(Catalogue& catalogue) const {
        if (initialControlState_) {
            catalogue.control(ControlAction::Enable, *initialControlState_);
        }
    }

    // Insert URIs
    void includeData(const eckit::URI& uri);
    void excludeData(const eckit::URI& uri);
    void markForMasking(Index idx) { indexesToMask_.insert(idx); }
    void info(const std::string& in) { info_ = in; }

    // Signing
    void signStoreStates(std::string secret);

    // Getters
    const Key& dbKey() const { return dbKey_; }
    StoreStates& storeStates() { return storeWipeStates_; }
    const std::set<Index>& indexesToMask() const;

    // Overrides
    void encode(eckit::Stream& s) const override;
    WipeElements extractWipeElements() override;

private:

    // For finding the catalogue again later.
    Key dbKey_;

    mutable std::unique_ptr<Catalogue> catalogue_;

    std::set<Index> indexesToMask_ = {};  // @todo, use this...
    StoreStates storeWipeStates_;

    std::string info_;  // Additional info about this particular catalogue (e.g. owner)

    // Used to reset control state in event of incomplete wipe
    std::optional<ControlIdentifiers> initialControlState_;
};

// -----------------------------------------------------------------------------------------------

}  // namespace fdb5