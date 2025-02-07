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

#ifndef fdb5_api_local_DumpVisitor_H
#define fdb5_api_local_DumpVisitor_H

#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"

namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class DumpVisitor : public QueryVisitor<DumpElement> {

public:

    DumpVisitor(eckit::Queue<DumpElement>& queue, const metkit::mars::MarsRequest& request, bool simple) :
        QueryVisitor<DumpElement>(queue, request), out_(new QueueStringLogTarget(queue)), simple_(simple) {}

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override {
        catalogue.dump(out_, simple_);
        return true;
    }

    bool visitIndex(const Index&) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    void visitDatum(const Field& field, const std::string& keyFingerprint) override {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }

private:

    eckit::Channel out_;
    bool simple_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
