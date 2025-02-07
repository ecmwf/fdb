/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Simon Smart
/// @date   August 2022

#pragma once

#include "eckit/distributed/Transport.h"
#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/EntryVisitMechanism.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MoveVisitor : public EntryVisitor {

public:  // methods

    MoveVisitor(const metkit::mars::MarsRequest& request, const eckit::URI& dest);

    ~MoveVisitor() override;

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitIndex(const Index& /*index*/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const Key& /**/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const std::string& /*keyFingerprint*/) override { NOTIMP; }

protected:  // members

    const metkit::mars::MarsRequest& request_;
    const eckit::URI& dest_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
