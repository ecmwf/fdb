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
#include "fdb5/api/SelectFDB.h"
#include "fdb5/io/HandleGatherer.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

static FDBBuilder<SelectFDB> selectFdbBuilder("select");

//----------------------------------------------------------------------------------------------------------------------


SelectFDB::SelectFDB(const Config& config) :
    FDBBase(config) {

    ASSERT(config.getString("type", "") == "select");

    if (!config.has("fdbs")) {
        throw eckit::UserError("fdbs not specified for select FDB", Here());
    }

    std::vector<eckit::LocalConfiguration> fdbs(config.getSubConfigurations("fdbs"));
    for (const eckit::LocalConfiguration& c : fdbs) {
        subFdbs_.push_back(FDB(c));
    }
}


SelectFDB::~SelectFDB() {}


void SelectFDB::archive(const Key& key, const void* data, size_t length) {

    for (FDB& fdb : subFdbs_) {
        if (fdb.matches(key)) {
            fdb.archive(key, data, length);
            return;
        }
    }

    std::stringstream ss;
    ss << "No matching fdb for key: " << key;
    throw eckit::UserError(ss.str(), Here());
}


eckit::DataHandle *SelectFDB::retrieve(const MarsRequest& request) {

    // TODO: Select within the SubFDBs

//    HandleGatherer result(true); // Sorted
    HandleGatherer result(false);

    for (FDB& fdb : subFdbs_) {
//        if (subFdbs_.matches()) {
        result.add(fdb.retrieve(request));
//        }
    }

    return result.dataHandle();
}

std::string SelectFDB::id() const {
    NOTIMP;
}


void SelectFDB::flush() {
    for (FDB& fdb : subFdbs_) {
        fdb.flush();
    }
}


void SelectFDB::print(std::ostream &s) const {
    s << "SelectFDB()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
