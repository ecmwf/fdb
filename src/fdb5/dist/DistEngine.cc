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
#include "fdb5/dist/DistEngine.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string DistEngine::name() const {
    return DistEngine::typeName();
}

std::string DistEngine::dbType() const {
    return DistEngine::typeName();
}

eckit::PathName DistEngine::location(const Key& key) const {
    NOTIMP;
}

bool DistEngine::canHandle(const eckit::PathName& path) const {
    // dist backend does not handle data by location, only by key.
    return false;
}

static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing) {
    NOTIMP;
}

std::vector<eckit::PathName> DistEngine::databases(const Key& key, const std::vector<eckit::PathName>& roots) {
    NOTIMP;
}


std::vector<eckit::PathName> DistEngine::allLocations(const Key& key) const {
    NOTIMP;
}

std::vector<eckit::PathName> DistEngine::visitableLocations(const Key& key) const {
    NOTIMP;
}

std::vector<eckit::PathName> DistEngine::writableLocations(const Key& key) const {
    NOTIMP;
}

void DistEngine::print(std::ostream& out) const {
    out << "DistEngine()";
}

static EngineBuilder<DistEngine> dist_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
