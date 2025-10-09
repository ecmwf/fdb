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

#ifndef fdb5_api_local_WipeVisitor_H
#define fdb5_api_local_WipeVisitor_H

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/WipeVisitor.h"

#include "eckit/filesystem/PathName.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class WipeVisitor : public QueryVisitor<WipeElement> {

public:  // methods

    WipeVisitor(eckit::Queue<WipeElement>& queue, const metkit::mars::MarsRequest& request, bool doit, bool porcelain,
                bool unsafeWipeAll);

    bool visitEntries() override { return false; }
    bool visitIndexes() override;
    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index& index) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const std::string& /*keyFingerprint*/) override { NOTIMP; }

    void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) override { throw e; }

private:  // members

    eckit::Channel out_;
    bool doit_;
    bool porcelain_;
    bool unsafeWipeAll_;

    std::unique_ptr<fdb5::WipeVisitor> internalVisitor_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
