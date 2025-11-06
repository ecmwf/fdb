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

Reindexer::Reindexer(const Config& dbConfig) : Archiver(dbConfig) {}

Reindexer::~Reindexer() {
    flush();
}

void Reindexer::reindex(const Key& key, const FieldLocation& fieldLocation) {
    ReindexVisitor visitor{*this, key, fieldLocation};
    archive(key, visitor);
}

void Reindexer::flushDatabase(Database& db) {
    db.catalogue_->flush(db.catalogue_->archivedLocations());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
