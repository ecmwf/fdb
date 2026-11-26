// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @author Simon Smart
/// @date   August 2022

#pragma once

#include "eckit/distributed/Transport.h"

#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/MoveVisitor.h"

#include "eckit/filesystem/PathName.h"


namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class MoveVisitor : public QueryVisitor<MoveElement> {

public:  // methods

    MoveVisitor(eckit::Queue<MoveElement>& queue, const metkit::mars::MarsRequest& request, const eckit::URI& dest);

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool supportsConcurrentIndexVisitation() const final { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;

    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>&) override { NOTIMP; }

    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const std::string& /*keyFingerprint*/) override {
        NOTIMP;
    }

private:  // members

    const eckit::URI& dest_;
    std::unique_ptr<fdb5::MoveVisitor> internalVisitor_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local
