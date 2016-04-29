/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/TocDBReader.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBReader::TocDBReader(const Key &key) :
    TocDB(key),
    indexes_(loadIndexes()) {
}

TocDBReader::~TocDBReader() {
    freeIndexes(indexes_);
}

bool TocDBReader::selectIndex(const Key &key) {
    currentIndexKey_ = key;

    for (std::vector<Index *>::iterator j = current_.begin(); j != current_.end(); ++j) {
        (*j)->close();
    }

    current_.clear();

    eckit::Log::info() << "TocDBReader::selectIndex " << key << std::endl;

    for (std::vector<Index *>::iterator j = indexes_.begin(); j != indexes_.end(); ++j) {
        if ((*j)->key() == key) {
            eckit::Log::info() << "Matching " << (*j)->key() << std::endl;
            current_.push_back(*j);
            (*j)->open();
        } else {
            eckit::Log::info() << "Not matching " << (*j)->key() << std::endl;
        }
    }

    eckit::Log::info() << "Found indexes " << current_.size() << std::endl;

    return (current_.size() != 0);
}

void TocDBReader::deselectIndex() {
    NOTIMP; //< should not be called
}

bool TocDBReader::open() {

    if (indexes_.empty()) {
        return false;
    }

    loadSchema();
    return true;
}

void TocDBReader::axis(const std::string &keyword, eckit::StringSet &s) const {
    for (std::vector<Index *>::const_iterator j = current_.begin(); j != current_.end(); ++j) {
        const eckit::StringSet &a = (*j)->axes().values(keyword);
        s.insert(a.begin(), a.end());
    }
}

void TocDBReader::close() {
    for (std::vector<Index *>::const_iterator j = current_.begin(); j != current_.end(); ++j) {
        (*j)->close();
    }
}

eckit::DataHandle *TocDBReader::retrieve(const Key &key) const {
    eckit::Log::info() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::info() << "Scanning indexes " << current_.size() << std::endl;

    Index::Field field;
    for (std::vector<Index *>::const_iterator j = current_.begin(); j != current_.end(); ++j) {
        if ((*j)->get(key, field)) {
            return field.path_.partHandle(field.offset_, field.length_);
        }
    }

    return 0;
}


void TocDBReader::print(std::ostream &out) const {
    out << "TocDBReader[]";
}

static DBBuilder<TocDBReader> builder("toc.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
