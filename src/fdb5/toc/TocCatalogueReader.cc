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
#include <vector>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocCatalogueReader::TocCatalogueReader(const Key& dbKey, const fdb5::Config& config) : TocCatalogue(dbKey, config) {
    loadIndexesAndRemap();
}

TocCatalogueReader::TocCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    TocCatalogue(uri.path(), ControlIdentifiers{}, config) {
    loadIndexesAndRemap();
}

TocCatalogueReader::~TocCatalogueReader() {
    LOG_DEBUG_LIB(LibFdb5) << "Closing DB " << *dynamic_cast<TocCatalogue*>(this) << std::endl;
}

void TocCatalogueReader::loadIndexesAndRemap() const {
    std::vector<Key> remapKeys;
    /// @todo: this should throw DatabaseNotFoundException if the toc file is not found
    std::vector<Index> indexes = loadIndexes(false, nullptr, nullptr, &remapKeys);

    ASSERT(remapKeys.size() == indexes.size());
    indexes_.reserve(remapKeys.size());
    for (size_t i = 0; i < remapKeys.size(); ++i) {
        indexes_.emplace_back(indexes[i], remapKeys[i]);
    }
}

bool TocCatalogueReader::selectIndex(const Key& idxKey) {

    if (currentIndexKey_ == idxKey) {
        return true;
    }

    currentIndexKey_ = idxKey;
    matching_.clear();

    for (auto& pair : indexes_) {
        if (pair.first.key() == idxKey) {
            matching_.push_back(&pair);
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "selectIndex " << idxKey << ", found " << matching_.size() << " matche(s)" << std::endl;

    return !matching_.empty();
}

void TocCatalogueReader::deselectIndex() {
    NOTIMP;  //< should not be called
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

bool TocCatalogueReader::axis(const std::string& keyword, eckit::DenseSet<std::string>& s) const {
    bool found = false;
    for (const auto* pair : matching_) {
        const auto& index = pair->first;
        if (index.axes().has(keyword)) {
            found = true;
            s.merge(index.axes().values(keyword));
        }
    }
    return found;
}

void TocCatalogueReader::close() {
    for (auto m = indexes_.begin(); m != indexes_.end(); ++m) {
        m->first.close();
    }
}

bool TocCatalogueReader::retrieve(const Key& key, Field& field) const {
    LOG_DEBUG_LIB(LibFdb5) << "Trying to retrieve key " << key << "  " << key.names() << std::endl;
    LOG_DEBUG_LIB(LibFdb5) << "Scanning indexes " << matching_.size() << std::endl;

    for (const auto& m : matching_) {
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

void TocCatalogueReader::print(std::ostream& out) const {
    out << "TocCatalogueReader(" << directory() << ")";
}

std::vector<Index> TocCatalogueReader::indexes(bool sorted) const {

    std::vector<Index> returnedIndexes;
    returnedIndexes.reserve(indexes_.size());
    for (const auto& pair : indexes_) {
        returnedIndexes.emplace_back(pair.first);
    }

    // If required, sort the indexes by file, and location within the file, for efficient iteration.
    if (sorted) {
        std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
    }

    return returnedIndexes;
}

static CatalogueReaderBuilder<TocCatalogueReader> builder("toc");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
