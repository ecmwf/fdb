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
/// @date   November 2018

#ifndef fdb5_api_local_WhereVisitor_H
#define fdb5_api_local_WhereVisitor_H

#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/WhereIterator.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class WhereVisitor : public QueryVisitor<WhereElement> {
public:
    using QueryVisitor<WhereElement>::QueryVisitor;
    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }
    bool visitDatabase(const DB& db) override { queue_.emplace(db.basePath()); return true; }
    bool visitIndex(const Index&) override { NOTIMP; }
    void visitDatum(const Field&, const Key&) override { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif
