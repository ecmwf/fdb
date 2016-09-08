/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemDB.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key) :
    DB(key) {
}

PMemDB::PMemDB(const eckit::PathName& directory) :
    DB(Key()) {
}

PMemDB::~PMemDB() {
}

bool PMemDB::open() {
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::archive(const Key &key, const void *data, Length length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle * PMemDB::retrieve(const Key &key) const {
    Log::error() << "retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::close() {
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::checkSchema(const Key &key) const {
    Log::error() << "checkSchema not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis not implemented for " << *this << std::endl;
    NOTIMP;
}

bool PMemDB::selectIndex(const Key &key) {
    Log::error() << "selectIndex not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::deselectIndex() {
    Log::error() << "deselectIndox not implemented for " << *this << std::endl;
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
