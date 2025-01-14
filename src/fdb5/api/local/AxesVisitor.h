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

#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/AxesIterator.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class AxesVisitor : public QueryVisitor<AxesElement> {
public:

    AxesVisitor(eckit::Queue<AxesElement>& queue,
                const metkit::mars::MarsRequest& request,
                const Config& config,
                int level);

    bool visitIndexes() override { return true; }
    bool visitEntries() override { return false; }
    void catalogueComplete(const fdb5::Catalogue& catalogue) override;
    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index&) override;
    void visitDatum(const Field&, const Key&) override { NOTIMP; }

private: // members

    Key dbKey_;
    IndexAxis axes_;
    const Schema& schema_;
    int level_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5
