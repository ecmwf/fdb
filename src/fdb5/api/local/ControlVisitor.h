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
/// @date   July 2019

#ifndef fdb5_api_local_ControlVisitor_H
#define fdb5_api_local_ControlVisitor_H

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/local/QueryVisitor.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class ControlVisitor : public QueryVisitor<ControlElement> {
public:

    ControlVisitor(eckit::Queue<ControlElement>& queue, const metkit::mars::MarsRequest& request, ControlAction action,
                   ControlIdentifiers identifiers);

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index&) override { NOTIMP; }

    void visitDatum(const Field&, const Key&) override { NOTIMP; }

private:  // members

    ControlAction action_;
    ControlIdentifiers identifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif
