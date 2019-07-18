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

#ifndef fdb5_api_local_WipeVisitor_H
#define fdb5_api_local_WipeVisitor_H

#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/helpers/WipeIterator.h"

#include "eckit/filesystem/PathName.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

// TODO: This WipeVisitor is really a TocWipeVisitor in disguise. This needs to be split out.

class WipeVisitor : public QueryVisitor<WipeElement> {
public:

    WipeVisitor(eckit::Queue<WipeElement>& queue,
                const metkit::MarsRequest& request,
                bool doit,
                bool porcelain);

    bool visitEntries() override { return false; }

    bool visitDatabase(const DB& db) override;
    bool visitIndex(const Index& index) override;
    void databaseComplete(const DB& db) override;
    void visitDatum(const Field&, const Key&) override { NOTIMP; }

    void visitDatum(const Field& field, const std::string& keyFingerprint) {
        EntryVisitor::visitDatum(field, keyFingerprint);
    }

private: // methods

    void report();
    void wipe(const DB& db);

private: // members

    metkit::MarsRequest indexRequest_;

    eckit::Channel out_;
    bool doit_;
    bool porcelain_;

    // Details of current DB being explored/wiped

    std::string owner_;
    eckit::PathName basePath_;

    std::set<eckit::PathName> metadataPaths_;
    std::set<eckit::PathName> dataPaths_;
    std::set<eckit::PathName> safePaths_;

    std::vector<Index> indexesToMask_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

#endif
