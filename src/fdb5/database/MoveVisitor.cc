/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/MoveVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MoveVisitor::MoveVisitor(const metkit::mars::MarsRequest& request,
                         const eckit::URI& dest,
                         bool removeSrc,
                         int removeDelay,
                         eckit::Transport& transport) :
    EntryVisitor(),
    request_(request),
    dest_(dest),
    removeSrc_(removeSrc),
    removeDelay_(removeDelay),
    transport_(transport) {}

MoveVisitor::~MoveVisitor() {}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
