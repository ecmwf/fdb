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
#include <unordered_map>
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"

class Catalogue;
class Index;

namespace fdb5 {

class WipeState {

public:

    WipeState(const Key& dbKey, const Config& config) : dbKey_(dbKey), config_(config) {}
    WipeState(WipeElements elements) : wipeElements_(std::move(elements)) {}

    WipeState(eckit::Stream& s) : dbKey_(s), config_() {  // XXX: Empty config(!!!). We may need it...
        size_t n;
        s >> n;
        for (size_t i = 0; i < n; ++i) {
            wipeElements_.emplace_back(std::make_shared<WipeElement>(s));
        }
        s >> n;
        for (size_t i = 0; i < n; ++i) {
            eckit::URI uri(s);
            includeDataURIs_.insert(uri);
        }
        s >> n;
        for (size_t i = 0; i < n; ++i) {
            eckit::URI uri(s);
            excludeDataURIs_.insert(uri);
        }
    }

    const WipeElements& wipeElements() const { return wipeElements_; }

    void insertWipeElement(const std::shared_ptr<WipeElement>& element) { wipeElements_.emplace_back(element); }

    const std::set<eckit::URI>& includeURIs() const { return includeDataURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeDataURIs_; }

    void include(const eckit::URI& uri) { includeDataURIs_.insert(uri); }

    void exclude(const eckit::URI& uri) { excludeDataURIs_.insert(uri); }

    /// XXX: Move to CatalogueWipeState?
    std::unique_ptr<Catalogue> getCatalogue() const {
        return CatalogueReaderFactory::instance().build(dbKey_, config_);
    }

    const Config& config() const { return config_; }

    // encode / decode
    void encode(eckit::Stream& s) const {
        s << dbKey_;
        // s << config_;
        s << static_cast<size_t>(wipeElements_.size());
        for (const auto& el : wipeElements_) {
            s << *el;
        }
        s << static_cast<size_t>(includeDataURIs_.size());
        for (const auto& uri : includeDataURIs_) {
            s << uri;
        }
        s << static_cast<size_t>(excludeDataURIs_.size());
        for (const auto& uri : excludeDataURIs_) {
            s << uri;
        }
    }

protected:

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;

    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;

    // For finding the catalogue again later.
    Key dbKey_;
    Config config_;
};


class StoreWipeState : public WipeState {

public:

    StoreWipeState(eckit::URI uri, const Config& config) :
        WipeState(Key(), config), storeURI_(uri), store_{StoreFactory::instance().build(storeURI_, config_)} {}

    Store& store() const { return *store_; }

    const eckit::URI& storeURI() const { return storeURI_; }

private:

    eckit::URI storeURI_;
    std::unique_ptr<Store> store_;
};


class CatalogueWipeState : public WipeState {
public:
    // consider not storing config in these classes. Just pass it by ref when needed.
    CatalogueWipeState(const Key& dbKey, const Config& cfg) : WipeState(dbKey, cfg) {}

    const std::vector<Index>& indexesToMask() const { return indexesToMask_; }

protected:
    std::vector<Index> indexesToMask_ = {};

};

class TocWipeState : public CatalogueWipeState {
public:

    TocWipeState(const Key& dbKey, const Config& cfg) : CatalogueWipeState(dbKey, cfg) {}

private:

    friend class TocCatalogue;

    // XXX ENSURE WE USE THESE!!!
    std::set<eckit::URI> subtocPaths_         = {};
    std::set<eckit::PathName> lockfilePaths_  = {};
    std::set<eckit::URI> indexPaths_          = {};
    std::set<eckit::URI> safePaths_           = {};
    std::set<eckit::PathName> residualPaths_  = {};
    std::set<eckit::PathName> cataloguePaths_ = {};
};


// ------------------------------------------------------------------------------------------------------

class WipeCoordinator {
public:

    WipeCoordinator() = default;

    // just a place for the wipe logic to live
    void wipe(eckit::Queue<WipeElement>& queue, const CatalogueWipeState& catalogueState, bool doit, bool unsafeWipeAll) const;
};


}  // namespace fdb5