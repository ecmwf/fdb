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

#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocCatalogue.h"
#include <memory>
#include <unordered_map>

class Catalogue;
class Index;

namespace fdb5 {

class WipeState {

public:

    WipeState(const Key& dbKey, const Config& config) : dbKey_(dbKey), config_(config) {}

    WipeElements& wipeElements() { return wipeElements_; } // Todo, make this const?
    const std::set<eckit::URI>& includeURIs() const { return includeDataURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeDataURIs_; }
    // const Catalogue& catalogue() const { return catalogue_; }

    void include(const eckit::URI& uri) {
        includeDataURIs_.insert(uri);
    }

    void exclude(const eckit::URI& uri) {
        excludeDataURIs_.insert(uri);
    }

    std::set<eckit::URI>& includeURIs() { return includeDataURIs_; }
    std::set<eckit::URI>& excludeURIs() { return excludeDataURIs_; }

    std::unique_ptr<Catalogue> getCatalogue() const {
        return CatalogueReaderFactory::instance().build(dbKey_, config_);
    }


    const Config& config() { return config_; }

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

    StoreWipeState(eckit::URI uri, const Config& config) : WipeState(Key(), config), storeURI_(uri),
        store_{StoreFactory::instance().build(storeURI_, config_)} {
    }
     
    Store & store() const {
        return *store_;
    }

    const eckit::URI& storeURI() const { return storeURI_; }

private:
    eckit::URI storeURI_;
    std::unique_ptr<Store> store_;
};

class TocWipeState: public WipeState {
public:

    TocWipeState(const Key& dbKey, const Config& cfg)
        : WipeState(dbKey, cfg) {}

private:

    friend class TocCatalogue;

    std::set<eckit::URI> subtocPaths_         = {};
    std::set<eckit::PathName> lockfilePaths_  = {};
    std::set<eckit::URI> indexPaths_          = {};
    std::set<eckit::URI> safePaths_           = {};
    std::set<eckit::PathName> residualPaths_  = {};
    std::vector<Index> indexesToMask_         = {};
    std::set<eckit::PathName> cataloguePaths_ = {};

};


// ------------------------------------------------------------------------------------------------------

class WipeCoordinator {
public:
    WipeCoordinator() = default;

    // just a place for the wipe logic to live
    void wipe(eckit::Queue<WipeElement>& queue, WipeState& catalogueState, bool doit, bool unsafeWipeAll) const;

};


} // namespace fdb5