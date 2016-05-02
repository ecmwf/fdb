/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/toc/TocDB.h"
#include "fdb5/rules/Rule.h"
#include "eckit/log/Timer.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key) :
    DB(key),
    TocHandler(directory(key)) {
}

TocDB::TocDB(const eckit::PathName& directory) :
    DB(Key()),
    TocHandler(directory) {
}

TocDB::~TocDB() {
}

void TocDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis() not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocDB::open() {
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::archive(const Key &key, const void *data, Length length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle *TocDB::retrieve(const Key &key) const {
    Log::error() << "Retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::close() {
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::loadSchema() {
    Timer timer("TocDB::loadSchema()");
    schema_.load( directory_ / "schema" );
}

void TocDB::checkSchema(const Key &key) const {
    Timer timer("TocDB::checkSchema()");
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
}

const Schema& TocDB::schema() const {
    return schema_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
