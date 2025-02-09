/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   January 2024

#pragma once

#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class Index;
class Field;
class Schema;
class Store;
class Catalogue;

namespace api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class AxesVisitor : public QueryVisitor<AxesElement> {
public:

    AxesVisitor(eckit::Queue<AxesElement>& queue, const metkit::mars::MarsRequest& request, int level);

    bool visitIndexes() override { return true; }

    bool visitEntries() override { return false; }

    void catalogueComplete(const Catalogue& catalogue) override;

    bool preVisitDatabase(const eckit::URI& uri, const Schema& schema) override;

    bool visitDatabase(const Catalogue& catalogue) override;

    bool visitIndex(const Index& index) override;

    void visitDatum(const Field& /*field*/, const Key& /*key*/) override { NOTIMP; }

private:  // members

    Key dbKey_;
    IndexAxis axes_;
    int level_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace api::local

}  // namespace fdb5
