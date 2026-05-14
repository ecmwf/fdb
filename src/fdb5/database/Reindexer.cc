/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Reindexer.h"
#include "fdb5/database/ReindexVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Reindexer::Reindexer(const std::string& tracingID, const Config& dbConfig) : Archiver(tracingID, dbConfig) {}

Reindexer::~Reindexer() {
    flush(tracingID_);
}

void Reindexer::reindex(const Key& key, const FieldLocation& fieldLocation, const std::string& tracingID) {
    ReindexVisitor visitor{*this, key, fieldLocation};
    archive(key, visitor, tracingID);
}

void Reindexer::flushDatabase(Database& db, const std::string& tracingID) {
    db.catalogue_->flush(db.catalogue_->archivedLocations(), tracingID);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
