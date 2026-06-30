/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
