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

    WipeState(const Key& dbKey) : dbKey_(dbKey) {}
    WipeState(WipeElements elements) : wipeElements_(std::move(elements)) {}

    WipeState(eckit::Stream& s) : dbKey_(s) {
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

    size_t encodeSize() const {
        // approximate... do this better...

        size_t size = 0;
        size += sizeof(size_t);  // key string size
        size += 256; // dbkey string (approx)
        size += sizeof(size_t);  // number of wipe elements
        for (const auto& el : wipeElements_) {
            size += el->encodeSize();
        }
        size += 2 * sizeof(size_t); // num include + num exclude
        size += (includeDataURIs_.size() + excludeDataURIs_.size()) * 256;
        return size;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const WipeState& state) {
        state.encode(s);
        return s;
    }

    const Key& dbKey() const { return dbKey_; }

    const WipeElements& wipeElements() const { return wipeElements_; }

    void insertWipeElement(const std::shared_ptr<WipeElement>& element) { wipeElements_.emplace_back(element); }

    const std::set<eckit::URI>& includeURIs() const { return includeDataURIs_; }
    const std::set<eckit::URI>& excludeURIs() const { return excludeDataURIs_; }

    void include(const eckit::URI& uri) { includeDataURIs_.insert(uri); }

    void exclude(const eckit::URI& uri) { excludeDataURIs_.insert(uri); }


    // encode / decode
    void encode(eckit::Stream& s) const {
        s << dbKey_;
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

    void print(std::ostream& out) const { // debugging...
        out << "WipeState(dbKey=" << dbKey_.valuesToString() << ", wipeElements=[";
        std::string sep = "";
        for (const auto& el : wipeElements_) {
            out << sep << *el;
            sep = ",";
        }
        out << "], includeDataURIs=[";
        sep = "";
        for (const auto& uri : includeDataURIs_) {
            out << sep << uri;
            sep = ",";
        }
        out << "], excludeDataURIs=[";
        sep = "";
        for (const auto& uri : excludeDataURIs_) {
            out << sep << uri;
            sep = ",";
        }
        out << "])";
    }

    const Key& dbkey() const { return dbKey_; }

protected:

    // I kinda don't like wipeElements at all.
    WipeElements wipeElements_;

    // Are these always just .data, or can they be other things?
    std::set<eckit::URI> includeDataURIs_;
    std::set<eckit::URI> excludeDataURIs_;

    // For finding the catalogue again later.
    Key dbKey_;
};


class StoreWipeState : public WipeState {

public:

    StoreWipeState(eckit::URI uri) :
        WipeState(Key()), storeURI_(uri) {}

    Store& store(const Config& config) const {
        if (!store_)
            store_ = StoreFactory::instance().build(storeURI_, config);
        return *store_;
    }

    const eckit::URI& storeURI() const { return storeURI_; }

private:

    eckit::URI storeURI_;
    mutable std::unique_ptr<Store> store_;
};


class CatalogueWipeState : public WipeState {
public:
    // consider not storing config in these classes. Just pass it by ref when needed.
    CatalogueWipeState(const Key& dbKey) : WipeState(dbKey) {}

    const std::vector<Index>& indexesToMask() const { return indexesToMask_; }

protected:
    std::vector<Index> indexesToMask_ = {};

};

class TocWipeState : public CatalogueWipeState {
public:

    TocWipeState(const Key& dbKey) : CatalogueWipeState(dbKey) {}

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

    WipeCoordinator(const Config& config) : config_(config) {}

    // just a place for the wipe logic to live
    void wipe(eckit::Queue<WipeElement>& queue, const CatalogueWipeState& catalogueState, bool doit, bool unsafeWipeAll) const;

    std::map<eckit::URI, std::unique_ptr<StoreWipeState>> getStoreStates(const WipeState& wipeState) const;

private:

    const Config& config_;
};


}  // namespace fdb5