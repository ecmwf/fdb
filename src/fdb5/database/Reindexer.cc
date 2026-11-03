// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
