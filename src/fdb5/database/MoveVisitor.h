// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

    IndexScopePtr visitIndex(const Index& /*index*/, const Rule& /*rule*/) override { NOTIMP; }

    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /**/) override { NOTIMP; }
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const std::string& /*keyFingerprint*/) override {
        NOTIMP;
    }

protected:  // members

    const metkit::mars::MarsRequest& request_;
    const eckit::URI& dest_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
