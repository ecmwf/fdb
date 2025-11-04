/*
 * (C) Copyright 1996- ECMWF.
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
#include <unordered_map>
#include "eckit/filesystem/URI.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"

class Catalogue;
class Index;

namespace fdb5 {

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

    const WipeElements& wipeElements() const { return wipeElements_; }
    void insertWipeElement(const std::shared_ptr<WipeElement>& element) {
        // failIfSigned(); // Modifying wipe elements is fine. Modifying include/exclude URIs is not.
        wipeElements_.emplace_back(element);
    }


    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

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

protected:

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;

    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;

    // For finding the catalogue again later.
    Signature signature_;
    Key dbKey_;

private:

    void failIfSigned() const;
};

/* ------------------------------ StoreWipeState ------------------------------ */

class StoreWipeState : public WipeState {
public:

    explicit StoreWipeState(eckit::URI uri);  // XXX: Empty key seems wrong.
    explicit StoreWipeState(eckit::Stream& s);

    Store& store(const Config& config) const;

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const StoreWipeState& state) {
        state.encode(s);
        return s;
    }

    const eckit::URI& storeURI() const { return storeURI_; }

private:

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;
};

class CatalogueWipeState : public WipeState {

    using StoreStates = std::map<eckit::URI, std::unique_ptr<StoreWipeState>>;

public:

    // consider not storing config in these classes. Just pass it by ref when needed.
    CatalogueWipeState(const Key& dbKey) : WipeState(dbKey) {}

    CatalogueWipeState(const Key& dbKey, WipeElements elements) :
        WipeState(dbKey, std::move(elements)) {}  // maybe only the catalogue needs this.


    CatalogueWipeState(eckit::Stream& s) : WipeState(s) {
        size_t n;
        s >> n;

        for (size_t i = 0; i < n; ++i) {
            eckit::URI uri(s);
            auto state = std::make_unique<StoreWipeState>(s);
            storeWipeStates_.emplace(uri, std::move(state));
        }
    }

    void encode(eckit::Stream& s) const {
        WipeState::encode(s);

        s << static_cast<size_t>(storeWipeStates_.size());
        for (const auto& [uri, state] : storeWipeStates_) {
            s << uri;
            s << *state;
        }
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const CatalogueWipeState& state) {
        state.encode(s);
        return s;
    }

    const std::vector<Index>& indexesToMask() const { return indexesToMask_; }

    // Use WipeState from the Catalogue to assign safe and data URIs to the corresponding stores
    void buildStoreStates() {
        ASSERT(storeWipeStates_.empty());

        // Build store wipe states from include/exclude URIs. Note, unsigned at this point.

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

        // catalogue has no need for the data uris anymore.
    }

    [[nodiscard]] StoreStates takeStoreStates() const {
        // Move out and reset the member to an empty map
        return std::exchange(storeWipeStates_, {});
    }

    StoreStates& storeStates() { return storeWipeStates_; }

    // Secret only known by the catalogue and store handlers.
    void signStoreStates(std::string secret) {
        std::cout << "ZZZ Signing CatalogueWipeState store states" << std::endl;
        for (auto& [uri, state] : storeWipeStates_) {
            state->sign(secret);
            std::cout << "ZZZ StoreWipeState for " << uri << " signature: " << state->signature().sig_ << std::endl;
        }

        // self sign. This shouldn't be needed. XXX
        sign(secret);

        std::cout << "ZZZ Catalogue signature: " << signature_.sig_ << std::endl;
    }

protected:

    std::vector<Index> indexesToMask_ = {};
    mutable StoreStates storeWipeStates_;
};

class TocWipeState : public CatalogueWipeState {
public:

    TocWipeState(const Key& dbKey) : CatalogueWipeState(dbKey) {}

private:

    friend class TocCatalogue;

    // XXX ENSURE WE USE THESE!!!
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

class WipeCoordinator {
public:

    WipeCoordinator(const Config& config) : config_(config) {}

    // just a place for the wipe logic to live

    // returns a WipeState to be used for reporting to the client.
    std::unique_ptr<CatalogueWipeState> wipe(const CatalogueWipeState& catalogueState, bool doit,
                                             bool unsafeWipeAll) const;

    std::map<eckit::URI, std::unique_ptr<StoreWipeState>> getStoreStates(const WipeState& wipeState) const;

private:

    const Config& config_;
};


}  // namespace fdb5