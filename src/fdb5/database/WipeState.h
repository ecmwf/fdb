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
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"


namespace fdb5 {

class Catalogue;
class Index;

class WipeElement;
using WipeElements = std::vector<std::shared_ptr<WipeElement>>;
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

    Signature(eckit::Stream& s) {
        s >> sig_;
        std::cout << "YYY Decoded Signature: " << sig_ << std::endl;
    }

    void sign(uint64_t sig) {
        ASSERT(sig != 0);
        sig_ = sig;
    }

    bool isSigned() const { return sig_ != 0; }

    friend eckit::Stream& operator<<(eckit::Stream& s, const Signature& sig) {
        s << sig.sig_;
        std::cout << "YYY Encoded Signature: " << sig.sig_ << std::endl;
        return s;
    }

    bool validSignature(uint64_t expected) const {
        std::cout << "YYY Validating Signature: got " << sig_ << " expected " << expected << std::endl;
        return sig_ == expected;
    }

public:

    uint64_t sig_{0};
};


class WipeState {
public:

    WipeState(const Key& dbKey);
    WipeState(const Key& dbKey, WipeElements elements);

    explicit WipeState(eckit::Stream& s);

    std::size_t encodeSize() const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state);

    const Key& dbKey() const { return dbKey_; }

    // trying to phase this out...
    const WipeElements& wipeElements() const { return wipeElements_; }
    void insertWipeElement(const std::shared_ptr<WipeElement>& element) {
        // failIfSigned(); // Modifying wipe elements is fine. Modifying include/exclude URIs is not.
        wipeElements_.emplace_back(element);
    }


    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

    virtual WipeElements generateWipeElements() const = 0;

    const std::set<eckit::URI>& includeURIs() const { return includeDataURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeDataURIs_; }

    void include(const eckit::URI& uri);
    void exclude(const eckit::URI& uri);

    // encode / decode
    void encode(eckit::Stream& s) const;

    void print(std::ostream& out) const;

    void sign(std::string secret);
    const Signature& signature() const { return signature_; }

    std::uint64_t hash(std::string secret) const;

    const std::set<eckit::URI>& unrecognisedURIs() const { return unknownURIs_; }
    void insertUnrecognised(const eckit::URI& uri) {
        failIfSigned();
        unknownURIs_.insert(uri);
    }

    virtual bool wipeAll() const {
        return safeURIs_.empty();  // not really a concept for the store.
    }


    // No more uris may be marked as safe or for deletion.
    void lock() {
        locked = true;

        // Also, perform a sanity check to ensure there is no overlap between the 3 sets.
        for (auto& uri : deleteURIs_) {
            ASSERT(safeURIs_.find(uri) == safeURIs_.end());
        }

        for (auto& uri : unknownURIs_) {
            ASSERT(safeURIs_.find(uri) == safeURIs_.end());
        }

        for (auto& uri : unknownURIs_) {
            ASSERT(deleteURIs_.find(uri) == deleteURIs_.end());
        }
    }


    const std::set<eckit::URI>& safeURIs() const { return safeURIs_; }

    const std::set<eckit::URI>& deleteURIs() const { return deleteURIs_; }

    bool isNotMarkedAsSafe(const eckit::URI& uri) const { return safeURIs().find(uri) == safeURIs().end(); }

    void markAsSafe(const std::set<eckit::URI>& uris) {
        ASSERT(!locked);
        safeURIs_.insert(uris.begin(), uris.end());
    }

    void markForDeletion(const std::set<eckit::URI>& uris) {
        ASSERT(!locked);
        deleteURIs_.insert(uris.begin(), uris.end());
    }

private:

    void failIfSigned() const;

protected:

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;

    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;

    std::set<eckit::URI> unknownURIs_;

    // For finding the catalogue again later.
    Signature signature_;
    Key dbKey_;

private:

    std::set<eckit::URI> safeURIs_;  // files explicitly not to be deleted. // <-- I dont think the store has any reason
                                     // to mark anything as safe. Just delete and unrecognised.
    std::set<eckit::URI> deleteURIs_;  // files that will be deleted. // <-- for the store, this is mostly predetermined
                                       // by the catalogue, with the exception of auxiliary files!.
    bool locked = false;
};

/* ------------------------------ StoreWipeState ------------------------------ */

class StoreWipeState : public WipeState {
public:

    explicit StoreWipeState(eckit::URI uri);  // XXX: Empty key seems wrong.
    explicit StoreWipeState(eckit::Stream& s);

    virtual ~StoreWipeState() = default;

    Store& store(const Config& config) const;

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const StoreWipeState& state) {
        state.encode(s);
        return s;
    }

    WipeElements generateWipeElements() const override;

    const eckit::URI& storeURI() const { return storeURI_; }

    const std::set<eckit::URI>& dataURIs() const { return dataURIs_; }
    void insertDataURI(const eckit::URI& uri) { dataURIs_.insert(uri); }

    const std::set<eckit::URI>& dataAuxiliaryURIs() const { return auxURIs_; }
    void insertAuxiliaryURI(const eckit::URI& uri) { auxURIs_.insert(uri); }

    bool ownsURI(const eckit::URI& uri) const;


private:

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;

    std::set<eckit::URI> dataURIs_;
    std::set<eckit::URI> auxURIs_;
};

class CatalogueWipeState : public WipeState {

    using StoreStates = std::map<eckit::URI, std::unique_ptr<StoreWipeState>>;

public:

    // CatalogueWipeState();
    explicit CatalogueWipeState(const Key& dbKey);
    CatalogueWipeState(const Key& dbKey, WipeElements elements);
    explicit CatalogueWipeState(eckit::Stream& s);

    virtual ~CatalogueWipeState() = default;

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const CatalogueWipeState& state);

    const std::vector<Index>& indexesToMask() const;

    void buildStoreStates();

    [[nodiscard]] StoreStates takeStoreStates() const;

    StoreStates& storeStates();

    void signStoreStates(std::string secret);

    WipeElements generateWipeElements() const override;

    bool ownsURI(const eckit::URI& uri) const;


protected:

    std::vector<Index> indexesToMask_ = {};
    mutable StoreStates storeWipeStates_;

    std::set<eckit::URI> catalogueURIs_;     // e.g. tocs, subtocs
    std::set<eckit::URI> auxCatalogueURIs_;  // e.g. schema, lock files.
    std::set<eckit::URI> indexURIs_;         // .index files
    std::string info_;                       // Additional info about this particular catalogue (e.g. owner)
};
class TocWipeState : public CatalogueWipeState {  // Im not entirely convinced I need this.
public:

    TocWipeState(const Key& dbKey) : CatalogueWipeState(dbKey) {}

private:

    friend class TocCatalogue;

    // XXX ENSURE WE USE THESE!!! Or remove them. I think many / all can be removed
    // Probably ought to be encoding them too, if the client cares. Maybe not.
    std::set<eckit::URI> subtocPaths_         = {};
    std::set<eckit::PathName> lockfilePaths_  = {};
    std::set<eckit::URI> indexPaths_          = {};
    std::set<eckit::URI> safePaths_           = {};
    std::set<eckit::PathName> residualPaths_  = {};
    std::set<eckit::PathName> cataloguePaths_ = {};


    /// maybe we can remove all of the above.
};


// ------------------------------------------------------------------------------------------------------

}  // namespace fdb5