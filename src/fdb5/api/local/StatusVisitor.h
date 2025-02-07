/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_local_StatusVisitor_H
#define fdb5_api_local_StatusVisitor_H

#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/local/QueryVisitor.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class StatusVisitor : public QueryVisitor<StatusElement> {
public:

    using QueryVisitor<StatusElement>::QueryVisitor;
    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }
    bool visitDatabase(const Catalogue& catalogue) override {
        queue_.emplace(catalogue);
        return true;
    }
    bool visitIndex(const Index&) override { NOTIMP; }
    void visitDatum(const Field&, const Key&) override { NOTIMP; }

    void visitDatum(const Field& field, const std::string& keyFingerprint) override {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
