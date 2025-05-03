/*
 * (C) Copyright 2018- ECMWF.
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

#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/MoveVisitor.h"

#include "eckit/filesystem/PathName.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class MoveVisitor : public QueryVisitor<MoveElement> {

public:  // methods

    MoveVisitor(eckit::Queue<MoveElement>& queue, const metkit::mars::MarsRequest& request, const eckit::URI& dest);

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;

    bool visitIndex(const Index& /*index*/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const std::string& /*keyFingerprint*/) override { NOTIMP; }

private:  // members

    const eckit::URI& dest_;
    std::unique_ptr<fdb5::MoveVisitor> internalVisitor_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5
