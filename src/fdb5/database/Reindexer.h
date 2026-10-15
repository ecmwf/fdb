// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Reindexer.h
/// @author Chris Bradley

#pragma once

#include "fdb5/database/Archiver.h"
#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// Used for writing fields into a catalogue without writin any data to a store.
class Reindexer : public Archiver {

public:  // methods

    Reindexer(const Config& dbConfig = Config().expandConfig());

    virtual ~Reindexer();

    // Write a field location to the catalogue without touching the store
    void reindex(const Key& key, const FieldLocation& fieldLocation);

protected:  // methods

    void flushDatabase(Database& db) override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
