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

#ifndef fdb5_api_local_StatsVisitor_H
#define fdb5_api_local_StatsVisitor_H

#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/database/StatsReportVisitor.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class StatsVisitor : public QueryVisitor<StatsElement> {
public:

    using QueryVisitor::QueryVisitor;

    void visitDatabase(const DB& db) override;
    void visitIndex(const Index& index) override;
    void databaseComplete(const DB& db) override;
    void visitDatum(const Field& field, const std::string& keyFingerprint) override;
    void visitDatum(const Field&, const Key&) override;

private: // members

    std::unique_ptr<StatsReportVisitor> internalVisitor_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif