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
#include "fdb5/api/LocalFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Retriever.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

static FDBBuilder<LocalFDB> localFdbBuilder("local");

//----------------------------------------------------------------------------------------------------------------------


void LocalFDB::archive(const Key& key, const void* data, size_t length) {

    if (!archiver_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new archiver" << std::endl;
        archiver_.reset(new Archiver(config_));
    }

    archiver_->archive(key, data, length);
}


eckit::DataHandle *LocalFDB::retrieve(const MarsRequest &request) {

    if (!retriever_) {
        eckit::Log::debug<LibFdb>() << *this << ": Constructing new retriever" << std::endl;
        retriever_.reset(new Retriever(config_));
    }

    MarsRequest e("environ");
    MarsTask task(request, e);

    return retriever_->retrieve(task);
}


void LocalFDB::flush() {
    if (archiver_) {
        archiver_->flush();
    }
}


void LocalFDB::print(std::ostream &s) const {
    s << "LocalFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
