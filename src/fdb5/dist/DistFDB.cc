/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/parser/Tokenizer.h"

#include "fdb5/LibFdb.h"
#include "fdb5/dist/DistFDB.h"
#include "fdb5/io/HandleGatherer.h"


namespace fdb5 {

static FDBBuilder<DistFDB> distFdbBuilder("dist");

//----------------------------------------------------------------------------------------------------------------------

DistFDB::DistFDB(const eckit::Configuration& config) :
    FDBBase(config) {

    ASSERT(config.getString("type", "") == "dist");

    // Configure the available lanes.

    if (!config.has("lanes")) throw eckit::UserError("No lanes configured for pool", Here());

    std::vector<eckit::LocalConfiguration> laneConfigs;
    laneConfigs = config.getSubConfigurations("lanes");

    for(const eckit::LocalConfiguration laneCfg : laneConfigs) {
        lanes_.push_back(FDB(laneCfg));
    }
}

DistFDB::~DistFDB() {}


void DistFDB::archive(const Key& key, const void* data, size_t length) {

    // TODO: Distributed Hash table
    for (FDB& lane : lanes_) {
        lane.archive(key, data, length);
    }
}


eckit::DataHandle *DistFDB::retrieve(const MarsRequest &request) {

    // TODO: Deduplication. Currently no masking.

//    HandleGatherer result(true); // Sorted
    HandleGatherer result(false);

    for (FDB& lane : lanes_) {
        result.add(lane.retrieve(request));
    }

    return result.dataHandle();
}


void DistFDB::flush() {
    for (FDB& lane : lanes_) {
        lane.flush();
    }
}


void DistFDB::print(std::ostream &s) const {
    s << "DistFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
