// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Christopher Bradley
/// @date   January 2025

#pragma once

#include "fdb5/api/helpers/Callback.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Reindexer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// This Visitor takes a field locations and writes their entry into a catalogue.
// It will never talk to a store.
class ReindexVisitor : public BaseArchiveVisitor {

public:  // methods

    ReindexVisitor(Reindexer& owner, const Key& initialFieldKey, const FieldLocation& fieldLocation);

protected:  // methods

    bool selectDatum(const Key& datumKey, const Key& fullKey) override;

    void print(std::ostream& out) const override;

private:  // members

    const FieldLocation& fieldLocation_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
