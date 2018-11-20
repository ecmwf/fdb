/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/LibFdb.h"
#include "fdb5/toc/TocDBReader.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBReader::TocDBReader(const Key& key, const eckit::Configuration& config) :
    TocDB(key, config),
    indexes_(loadIndexes()) {
}

TocDBReader::TocDBReader(const eckit::PathName& directory, const eckit::Configuration& config) :
    TocDB(directory, config),
    indexes_(loadIndexes()) {
}


TocDBReader::~TocDBReader() {
    eckit::Log::debug<LibFdb>() << "Closing DB " << *this << std::endl;
}

bool TocDBReader::selectIndex(const Key &key) {

    if(currentIndexKey_ == key) {
        return true;
    }

    currentIndexKey_ = key;

    for (std::vector<Index>::iterator j = matching_.begin(); j != matching_.end(); ++j) {
        j->close();
    }

    matching_.clear();


    for (std::vector<Index>::iterator j = indexes_.begin(); j != indexes_.end(); ++j) {
        if (j->key() == key) {
//            eckit::Log::debug<LibFdb>() << "Matching " << j->key() << std::endl;
            matching_.push_back(*j);
//            j->open();
        }
//        else {
//           eckit::Log::info() << "Not matching " << j->key() << std::endl;
//        }
    }

    eckit::Log::debug<LibFdb>() << "TocDBReader::selectIndex " << key << ", found "
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
    for (std::vector<Index>::const_iterator j = matching_.begin(); j != matching_.end(); ++j) {
        const eckit::StringSet& a = j->axes().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocDBReader::close() {
    for (std::vector<Index>::iterator j = matching_.begin(); j != matching_.end(); ++j) {
        j->close();
    }
}

eckit::DataHandle *TocDBReader::retrieve(const Key &key) const {

    eckit::Log::debug<LibFdb>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb>() << "Scanning indexes " << matching_.size() << std::endl;

    Field field;
    for (std::vector<Index>::const_iterator j = matching_.begin(); j != matching_.end(); ++j) {
        if (j->mayContain(key)) {
            const_cast<Index*>(&(*j))->open();
            if (j->get(key, field)) {
                eckit::Log::debug<LibFdb>() << "FOUND KEY " << key << " -> " << *j << " " << field << std::endl;
                return field.dataHandle();
            }
        }
    }

    return 0;
}


void TocDBReader::print(std::ostream &out) const {
    out << "TocDBReader(" << directory() << ")";
}

std::vector<Index> TocDBReader::indexes(bool sorted) const {

    // If required, sort the indexes by file, and location within the file, for efficient iteration.
    if (sorted) {
        std::vector<Index> returnedIndexes(indexes_);
        std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
    }

    return indexes_;
}

static DBBuilder<TocDBReader> builder("toc.reader", true, false);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
