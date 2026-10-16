/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "fdb5/api/helpers/AccessIterator.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/local/QueryVisitor.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

class TouchVisitor : public QueryVisitor<ControlElement> {
public:

    TouchVisitor(eckit::Queue<ControlElement>& queue, const metkit::mars::MarsRequest& request,
                 AccessIdentifiers identifiers, AccessTime when);

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;
    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>&) override { NOTIMP; }

    using QueryVisitor<ControlElement>::visitDatum;
    void visitDatum(IndexScope&, const Field&, const Key&) override { NOTIMP; }

private:  // members

    AccessIdentifiers identifiers_;
    AccessTime when_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5
