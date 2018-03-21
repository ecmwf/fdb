/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

#include "fdb5/LibFdb.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/dist/DistDB.h"
#include "fdb5/dist/DistManager.h"
#include "fdb5/config/FDBConfig.h"
#include "fdb5/database/DbStats.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------



//----------------------------------------------------------------------------------------------------------------------

DistDB::DistDB(const Key& key, const eckit::Configuration& config) :
    DB(key) {

    eckit::Log::info() << "Constructed DistDB; key=" << key << std::endl;

    DistManager mgr(config);

    mgr.pool(key);

}

DistDB::DistDB(const eckit::PathName& directory, const eckit::Configuration& config) :
    DB(Key()) {

    throw eckit::SeriousBug("Opening by directory has no meaning for dist FDB", Here());
}

DistDB::~DistDB() {
}

void DistDB::axis(const std::string&, eckit::StringSet&) const {
    NOTIMP;
}

bool DistDB::open() {
    NOTIMP;
}

bool DistDB::exists() const {
    NOTIMP;
}

void DistDB::archive(const Key&, const void*, Length) {
    NOTIMP;
}

void DistDB::flush() {
    NOTIMP;
}

eckit::DataHandle *DistDB::retrieve(const Key&) const {
    NOTIMP;
}

void DistDB::close() {
    NOTIMP;
}

void DistDB::dump(std::ostream &out, bool simple) {
    NOTIMP;
}

std::string DistDB::owner() const {
    NOTIMP;
}

const Schema& DistDB::schema() const {
    NOTIMP;
}

eckit::PathName DistDB::basePath() const {
    NOTIMP;
}

void DistDB::visitEntries(EntryVisitor& visitor, bool sorted) {
    NOTIMP;
}

void DistDB::loadSchema() {
    NOTIMP;
}

void DistDB::checkSchema(const Key &key) const {
    NOTIMP;
}

DbStats DistDB::statistics() const {
    NOTIMP;
}

std::vector<Index> DistDB::indexes(bool sorted) const {
    NOTIMP;
}

void DistDB::visit(DBVisitor &visitor) {
    NOTIMP;
}

std::string DistDB::dbType() const {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
