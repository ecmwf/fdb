// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   July 2019

#pragma once

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/local/QueryVisitor.h"


namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class ControlVisitor : public QueryVisitor<ControlElement> {
public:

    ControlVisitor(eckit::Queue<ControlElement>& queue, const metkit::mars::MarsRequest& request, ControlAction action,
                   ControlIdentifiers identifiers);

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;
    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>&) override { NOTIMP; }

    using QueryVisitor<ControlElement>::visitDatum;
    void visitDatum(IndexScope&, const Field&, const Key&) override { NOTIMP; }

private:  // members

    ControlAction action_;
    ControlIdentifiers identifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local
