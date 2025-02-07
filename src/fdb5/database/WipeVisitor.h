/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   August 2019

#ifndef fdb5_database_WipeVisitor_H
#define fdb5_database_WipeVisitor_H

#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/EntryVisitMechanism.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class WipeVisitor : public EntryVisitor {

public:  // methods

    WipeVisitor(const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain,
                bool unsafeWipeAll);

    ~WipeVisitor() override;

    bool visitEntries() override { return false; }

    void visitDatum(const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    void visitDatum(const Field& /*field*/, const std::string& /*keyFingerprint*/) override { NOTIMP; }

protected:  // members

    const metkit::mars::MarsRequest& request_;

    std::ostream& out_;
    const bool doit_;
    const bool porcelain_;
    const bool unsafeWipeAll_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_WipeVisitor_H
