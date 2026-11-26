// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/MoveVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MoveVisitor::MoveVisitor(const metkit::mars::MarsRequest& request, const eckit::URI& dest) :
    EntryVisitor(), request_(request), dest_(dest) {}

MoveVisitor::~MoveVisitor() {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
