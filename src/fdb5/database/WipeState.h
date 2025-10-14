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

// class StoreWipeState;

class WipeReport {

public:
    WipeReport() = default;
    ~WipeReport() = default;

    std::vector<std::unique_ptr<WipeElement>> toWipeElements() {
        std::vector<std::unique_ptr<fdb5::WipeElement>> wipeElements;

        wipeElements.push_back(std::make_unique<WipeElement>(WipeElementType::WIPE_CATALOGUE_INFO, info_, std::set<eckit::URI>{}));

        wipeElements.push_back(std::make_unique<WipeElement>(
            WipeElementType::WIPE_CATALOGUE_SAFE, "Protected files (explicitly untouched):", std::move(safeURIs_)));

        
        if (wipeall_) {
            
            wipeElements.push_back(std::make_unique<WipeElement>(WipeElementType::WIPE_CATALOGUE,
                "Toc files to delete:", std::move(catalogueURIs_)));

            wipeElements.push_back(std::make_unique<WipeElement>(WipeElementType::WIPE_CATALOGUE_AUX,
                "Control files to delete:", std::move(auxURIs_)));

            if (!unknownURIs_.empty()) {
                wipeElements.push_back(std::make_unique<WipeElement>(
                    WipeElementType::WIPE_UNKNOWN, "Unexpected files present in the catalogue:", std::move(unknownURIs_)));
            }
        }


        wipeElements.push_back(
            std::make_unique<WipeElement>(WipeElementType::WIPE_CATALOGUE, "Index files to delete:", std::move(indexURIs_)));


        return wipeElements;
    }

private:
    bool wipeall_ = true;
    std::string info_;
    std::set<eckit::URI> safeURIs_;
    std::set<eckit::URI> catalogueURIs_;
    std::set<eckit::URI> auxURIs_;
    std::set<eckit::URI> unknownURIs_;
    std::set<eckit::URI> indexURIs_;
};

class WipeState {

public:
    // WipeState() = default; // can I remove?
    WipeState(const Key& dbKey, const Config& config) : cat_dbKey_(dbKey), cat_config_(config) {}

    WipeElements& wipeElements() { return wipeElements_; } // Todo, make this const?
    WipeElements& outElements() { return outElements_; } // Todo, make this const?
    const std::set<eckit::URI>& includeURIs() const { return includeDataURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeDataURIs_; }
    // const Catalogue& catalogue() const { return catalogue_; }

    void include(const eckit::URI& uri) {
        std::cout << "xxx Including URI: " << uri << std::endl;
        includeDataURIs_.insert(uri);
    }

    void exclude(const eckit::URI& uri) {
        std::cout << "xxx Excluding URI: " << uri << std::endl;
        excludeDataURIs_.insert(uri);
    }

    std::set<eckit::URI>& includeURIs() { return includeDataURIs_; }
    std::set<eckit::URI>& excludeURIs() { return excludeDataURIs_; }


    const Config& config() { return cat_config_; }


    // void assignToStores(const Catalogue& catalogue, const std::vector<eckit::URI>& dataURIs, bool include) {
    //     for (const auto& dataURI : dataURIs) {
    //         auto storeURI = StoreFactory::instance().uri(dataURI);
    //         auto it       = storeStates_.find(storeURI);
            
    //     }
    // }

protected:

    // We need to tell the catalogue to doit().
    // const Catalogue& catalogue_;

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;


    WipeElements outElements_; // temp...


    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;


    // as we need to contact after visiting for doit...
    Key cat_dbKey_;
    Config cat_config_;
    
    // store uri -> wipe state
    // std::map<eckit::URI, StoreWipeState> storeStates_;
};


class StoreWipeState {
public:
    // why do we need the config from the catalogue?
    StoreWipeState(eckit::URI uri, const Config& config) : storeURI_(uri), config_(config) {}

    WipeElements& wipeElements() { return wipeElements_; }

    // hopefully never need this in the end... If so, should be memoized
    std::unique_ptr<Store> getStore() const {
        return StoreFactory::instance().build(storeURI_, config_);
    }

    void addDataURI(const eckit::URI& uri) { dataURIs_.insert(uri); }
    void addSafeURI(const eckit::URI& uri) { safeURIs_.insert(uri); }

    const eckit::URI& storeURI() const { return storeURI_; }
    const std::set<eckit::URI>& dataURIs() const { return dataURIs_; }
    const std::set<eckit::URI>& safeURIs() const { return safeURIs_; }

    private:

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;

    eckit::URI storeURI_;
    Config config_;

    std::set<eckit::URI> dataURIs_ = {};
    std::set<eckit::URI> safeURIs_ = {};

};


class TocWipeState: public WipeState {
public:

    TocWipeState(const Key& dbKey, const Config& cfg)
        : WipeState(dbKey, cfg) {}



private:

    friend class TocCatalogue;

    // but of course, these are catalogue specific. I dont think the object used by the store should be the same as the one used by the catalogue
    std::set<eckit::URI> subtocPaths_         = {};
    std::set<eckit::PathName> lockfilePaths_  = {};
    std::set<eckit::URI> indexPaths_          = {};
    std::set<eckit::URI> safePaths_           = {};
    std::set<eckit::PathName> residualPaths_  = {};
    std::vector<Index> indexesToMask_         = {};
    std::set<eckit::PathName> cataloguePaths_ = {};

};

} // namespace fdb5