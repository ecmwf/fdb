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
#include "fdb5/toc/TocDBReader.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBReader::TocDBReader(const Key& key, const eckit::Configuration& config) :
    TocDB(key, config) {
    loadIndexesAndRemap();
}

TocDBReader::TocDBReader(const eckit::PathName& directory, const eckit::Configuration& config) :
    TocDB(directory, config) {
    loadIndexesAndRemap();
}


TocDBReader::~TocDBReader() {
    eckit::Log::debug<LibFdb5>() << "Closing DB " << *this << std::endl;
}

void TocDBReader::loadIndexesAndRemap() {
    std::vector<Key> remapKeys;
    std::vector<Index> indexes = loadIndexes(false, nullptr, nullptr, &remapKeys);

    ASSERT(remapKeys.size() == indexes.size());
    indexes_.reserve(remapKeys.size());
    for (size_t i = 0; i < remapKeys.size(); ++i) {
        indexes_.emplace_back(indexes[i], remapKeys[i]);
    }
}

bool TocDBReader::selectIndex(const Key &key) {

    if(currentIndexKey_ == key) {
        return true;
    }

    currentIndexKey_ = key;

    for (auto& idx : matching_) {
        idx.first.close();
    }

    matching_.clear();


    for (const auto& idx : indexes_) {
        if (idx.first.key() == key) {
            matching_.push_back(idx);
        }
    }

    eckit::Log::debug<LibFdb5>() << "TocDBReader::selectIndex " << key << ", found "
                                << matching_.size() << " matche(s)" << std::endl;

    return (matching_.size() != 0);
}

void TocDBReader::deselectIndex() {
    NOTIMP; //< should not be called
}

bool TocDBReader::open() {

    // This used to test if indexes_.empty(), but it is perfectly valid to have a DB with no indexes
    // if it has been created with fdb-root --create.
    // See MARS-

    if (!exists()) {
        return false;
    }

    loadSchema();
    return true;
}

void TocDBReader::axis(const std::string &keyword, eckit::StringSet &s) const {
    for (const auto& m : matching_ ){
        const eckit::StringSet& a = m.first.axes().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocDBReader::close() {
    for (auto& m : matching_) {
        m.first.close();
    }
}

eckit::DataHandle *TocDBReader::retrieve(const Key &key) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning indexes " << matching_.size() << std::endl;

    Field field;
    for (const auto& m : matching_) {
        const Index& idx(m.first);
        const Key& remapKey(m.second);

        if (idx.mayContain(key)) {
            const_cast<Index&>(idx).open();
            if (idx.get(key, field)) {
                eckit::Log::debug<LibFdb5>() << "INDEX: " << idx.key() << " : " << remapKey << std::endl;
                eckit::Log::debug<LibFdb5>() << "FOUND KEY " << key << " -> " << idx << " " << field << std::endl;
                return field.dataHandle(remapKey);
            }
        }
    }

    return 0;
}


void TocDBReader::print(std::ostream &out) const {
    out << "TocDBReader(" << directory() << ")";
}

std::vector<Index> TocDBReader::indexes(bool sorted) const {

    std::vector<Index> returnedIndexes;
    returnedIndexes.reserve(indexes_.size());
    for (const auto& idx : indexes_) {
        returnedIndexes.emplace_back(idx.first);
    }

    // If required, sort the indexes by file, and location within the file, for efficient iteration.
    if (sorted) {
        std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
    }

    return returnedIndexes;
}

static DBBuilder<TocDBReader> builder("toc.reader", true, false);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
