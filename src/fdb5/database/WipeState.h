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
#include "fdb5/database/Signature.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"


namespace fdb5 {

class CatalogueWipeState;
using WipeStateIterator = APIIterator<CatalogueWipeState>;
using URIMap            = std::map<WipeElementType, std::set<eckit::URI>>;

// -----------------------------------------------------------------------------------------------
/// Class for storing all URIs to be wiped.
/// There are several categories of URIs:
/// 1) URIs to be deleted, because they match against the wipe request.
/// 2) URIs marked as safe (not to be deleted). Typically these will be other URIs in the same DB, but did not match the
///    request.
/// 3) "Unknown URIs" - If we are wiping an entire DB, any remaining files on disk are "unknown" to the
///    catalogue/store. Their presence will cause the wipe to abort unless they can be associated with another store, or
///    --unsafe-wipe-all is specified.
class WipeState {
public:

    WipeState();

    WipeState(std::set<eckit::URI> safeURIs, URIMap deleteURIs) :
        deleteURIs_(std::move(deleteURIs)), safeURIs_(std::move(safeURIs)) {}

    explicit WipeState(eckit::Stream& s);

    WipeState(const WipeState&)            = delete;
    WipeState& operator=(const WipeState&) = delete;

    WipeState(WipeState&&) noexcept            = default;
    WipeState& operator=(WipeState&&) noexcept = default;

    virtual ~WipeState() = default;

    virtual void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

    void insertUnrecognised(const eckit::URI& uri) { unknownURIs_.insert(uri); }

    bool isMarkedForDeletion(const eckit::URI& uri) const;
    bool isMarkedSafe(const eckit::URI& uri) const;

    void markAsSafe(const std::set<eckit::URI>& uris) { safeURIs_.insert(uris.begin(), uris.end()); }

    void markForDeletion(WipeElementType type, const std::set<eckit::URI>& uris) {
        for (const auto& uri : uris) {
            markForDeletion(type, uri);
        }
    }

    void markForDeletion(WipeElementType type, const eckit::URI& uri) {
        if (isMarkedSafe(uri)) {
            return;
        }
        deleteURIs_[type].insert(uri);
    }

    // Ensure there are no overlaps between safe URIs and URIs to be deleted.
    void sanityCheck() const;

    // Getters
    const std::set<eckit::URI>& unrecognisedURIs() const { return unknownURIs_; }
    const std::set<eckit::URI>& safeURIs() const { return safeURIs_; }
    const URIMap& deleteMap() const { return deleteURIs_; }


    // Create WipeElements from this Class's contents.
    // Note: this moves the data out of this class and into the WipeElements. The class is not intended to be used after
    // this function call.
    virtual WipeElements extractWipeElements() = 0;

protected:

    URIMap deleteURIs_;
    std::set<eckit::URI> safeURIs_;

private:

    std::set<eckit::URI> unknownURIs_;
};

// -----------------------------------------------------------------------------------------------

class StoreWipeState : public WipeState {
public:

    StoreWipeState() = default;
    StoreWipeState(eckit::URI uri);
    StoreWipeState(eckit::Stream& s);

    // Non-copyable
    StoreWipeState(const StoreWipeState&)            = delete;
    StoreWipeState& operator=(const StoreWipeState&) = delete;

    // Movable
    StoreWipeState(StoreWipeState&&) noexcept            = default;
    StoreWipeState& operator=(StoreWipeState&&) noexcept = default;

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
        markAsSafe({uri});
    }

    // Remove URI from list of URIs to be deleted because it turns out not exist.
    // For remote stores, we must store this information to avoid reporting missing URIs to the client.
    /// @note: intentionally do not fail if signed, stores are allowed to uninclude URIs.
    void markAsMissing(const eckit::URI& uri) {
        missingURIs_.insert(uri);
        deleteURIs_[WipeElementType::STORE].erase(uri);
    }

    // Mark all data URIs as missing
    void markAllMissing() {
        for (const auto& uri : includedDataURIs()) {
            missingURIs_.insert(uri);
        }
        deleteURIs_.erase(WipeElementType::STORE);
    }

    // Signing

    void sign(const std::string& secret) const;
    std::uint64_t hash(const std::string& secret) const;

    // Getters

    Store& store(const Config& config) const;
    const eckit::URI& storeURI() const { return storeURI_; }
    const Signature& signature() const { return signature_; }

    const std::set<eckit::URI>& includedDataURIs() const {
        static const std::set<eckit::URI> empty{};
        auto it = deleteURIs_.find(WipeElementType::STORE);
        return (it != deleteURIs_.end()) ? it->second : empty;
    }

    const std::set<eckit::URI>& dataAuxiliaryURIs() const {
        static const std::set<eckit::URI> empty{};
        auto it = deleteURIs_.find(WipeElementType::STORE_AUX);
        return (it != deleteURIs_.end()) ? it->second : empty;
    }

    const std::set<eckit::URI>& missingURIs() const { return missingURIs_; }

    // Overrides
    void encode(eckit::Stream& s) const override;
    WipeElements extractWipeElements() override;

private:

    void failIfSigned() const;

private:

    mutable Signature signature_;

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;
    std::set<eckit::URI> missingURIs_;
};

// -----------------------------------------------------------------------------------------------

class CatalogueWipeState : public WipeState {

    using StoreStates = std::map<eckit::URI, std::unique_ptr<StoreWipeState>>;

public:

    /// @todo: Can we remove some of these constructors?

    CatalogueWipeState() : WipeState() {}

    CatalogueWipeState(const Key& dbKey) : WipeState(), dbKey_(dbKey) {}

    CatalogueWipeState(const Key& dbKey, std::set<eckit::URI> safeURIs, URIMap deleteURIs) :
        WipeState(std::move(safeURIs), std::move(deleteURIs)), dbKey_(dbKey) {}

    CatalogueWipeState(eckit::Stream& s);

    // Non-copyable
    CatalogueWipeState(const CatalogueWipeState&)            = delete;
    CatalogueWipeState& operator=(const CatalogueWipeState&) = delete;

    // Movable
    CatalogueWipeState(CatalogueWipeState&&) noexcept            = default;
    CatalogueWipeState& operator=(CatalogueWipeState&&) noexcept = default;

    virtual ~CatalogueWipeState() override {
        try {
            restoreControlState();
        }
        catch (...) {
            eckit::Log::warning() << "Failed to restore control state CatalogueWipeState (db " << dbKey_ << ")"
                                  << std::endl;
        }
    }

    Catalogue& catalogue(const Config& config) const {
        if (!catalogue_) {
            catalogue_ = CatalogueReaderFactory::instance().build(dbKey_, config);
        }
        return *catalogue_;
    }

    void initialControlState(const ControlIdentifiers& ids) { initialControlState_ = ids; }

    // Restore the control state (i.e. locks) of the catalogue to their pre-wipe state.
    void restoreControlState() {
        if (catalogue_ && initialControlState_) {
            catalogue_->control(ControlAction::Enable, *initialControlState_);
            initialControlState_.reset();  // only restore once
        }
    }

    void clearControlState() const { initialControlState_.reset(); }

    // Insert URIs

    /// Include data URI in the corresponding store wipe state.
    void includeData(const eckit::URI& uri);

    /// Exclude data URI in the corresponding store wipe state.
    void excludeData(const eckit::URI& uri);

    /// Mark an index to be masked as part of the wipe.
    void markForMasking(Index idx) { indexesToMask_.insert(idx); }

    void setInfo(const std::string& in) { info_ = in; }

    // Signing
    void signStoreStates(std::string secret) const;

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
    mutable std::optional<ControlIdentifiers> initialControlState_;
};

// -----------------------------------------------------------------------------------------------

}  // namespace fdb5