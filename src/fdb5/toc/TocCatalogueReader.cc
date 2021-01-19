/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocCatalogueReader::TocCatalogueReader(const Key& key, const fdb5::Config& config) :
    TocCatalogue(key, config) {
    loadIndexesAndRemap();
}

TocCatalogueReader::TocCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    TocCatalogue(uri.path(), config) {
    loadIndexesAndRemap();
}

TocCatalogueReader::~TocCatalogueReader() {
    eckit::Log::debug<LibFdb5>() << "Closing DB " << *dynamic_cast<TocCatalogue*>(this) << std::endl;
}

void TocCatalogueReader::loadIndexesAndRemap() {
    std::vector<Key> remapKeys;
    std::vector<Index> indexes = loadIndexes(false, nullptr, nullptr, &remapKeys);

    ASSERT(remapKeys.size() == indexes.size());
    indexes_.reserve(remapKeys.size());
    for (size_t i = 0; i < remapKeys.size(); ++i) {
        indexes_.emplace_back(indexes[i], remapKeys[i]);
    }
}

bool TocCatalogueReader::selectIndex(const Key &key) {

    if(currentIndexKey_ == key) {
        return true;
    }

    currentIndexKey_ = key;

    for (auto idx = matching_.begin(); idx != matching_.end(); ++idx) {
        idx->first.close();
    }

    matching_.clear();


    for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
        if (idx->first.key() == key) {
            matching_.push_back(*idx);
        }
    }

    eckit::Log::debug<LibFdb5>() << "TocCatalogueReader::selectIndex " << key << ", found "
                                << matching_.size() << " matche(s)" << std::endl;

    return (matching_.size() != 0);
}

void TocCatalogueReader::deselectIndex() {
    NOTIMP; //< should not be called
}

bool TocCatalogueReader::open() {

    // This used to test if indexes_.empty(), but it is perfectly valid to have a DB with no indexes
    // if it has been created with fdb-root --create.
    // See MARS-

    if (!TocCatalogue::exists()) {
        return false;
    }

    TocCatalogue::loadSchema();
    return true;
}

void TocCatalogueReader::axis(const std::string &keyword, eckit::StringSet &s) const {
    for (auto m = matching_.begin(); m != matching_.end(); ++m) {
        const eckit::DenseSet<std::string>& a = m->first.axes().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocCatalogueReader::close() {
    for (auto m = matching_.begin(); m != matching_.end(); ++m) {
        m->first.close();
    }
}

bool TocCatalogueReader::retrieve(const Key& key, Field& field) const {
    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning indexes " << matching_.size() << std::endl;

    for (auto m = matching_.begin(); m != matching_.end(); ++m) {
        const Index& idx(m->first);
        Key remapKey = m->second;

        if (idx.mayContain(key)) {
            const_cast<Index&>(idx).open();
            if (idx.get(key, remapKey, field)) {
                return true;
            }
        }
    }
    return false;
}

void TocCatalogueReader::print(std::ostream &out) const {
    out << "TocCatalogueReader(" << directory() << ")";
}

std::vector<Index> TocCatalogueReader::indexes(bool sorted) const {

    std::vector<Index> returnedIndexes;
    returnedIndexes.reserve(indexes_.size());
    for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
        returnedIndexes.emplace_back(idx->first);
    }

    // If required, sort the indexes by file, and location within the file, for efficient iteration.
    if (sorted) {
        std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
    }

    return returnedIndexes;
}

static CatalogueBuilder<TocCatalogueReader> builder("toc.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
