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

#ifndef fdb5_TocWipeVisitor_H
#define fdb5_TocWipeVisitor_H


#include "fdb5/database/WipeVisitor.h"

namespace fdb5 {

class TocDB;

//----------------------------------------------------------------------------------------------------------------------


class TocWipeVisitor : public WipeVisitor {

public:

    TocWipeVisitor(const TocDB& db,
                   const metkit::MarsRequest& request,
                   std::ostream& out,
                   bool doit,
                   bool porcelain,
                   bool unsafeWipeAll);
    ~TocWipeVisitor() override;

private: // methods

    bool visitDatabase(const DB& db) override;
    bool visitIndex(const Index& index) override;
    void databaseComplete(const DB& db) override;

    void addMaskedPaths();
    void addMetadataPaths();
    void ensureSafePaths();
    void calculateResidualPaths();

    bool anythingToWipe() const;

    void report();
    void wipe(bool wipeAll);

private: // members

    // What are the parameters of the wipe operation
    const TocDB& db_;

    metkit::MarsRequest indexRequest_;

    std::string owner_;
    eckit::PathName basePath_;

    eckit::PathName tocPath_;
    eckit::PathName schemaPath_;

    std::set<eckit::PathName> subtocPaths_;
    std::set<eckit::PathName> lockfilePaths_;
    std::set<eckit::PathName> indexPaths_;
    std::set<eckit::PathName> dataPaths_;

    std::set<eckit::PathName> safePaths_;
    std::set<eckit::PathName> residualPaths_;

    std::vector<Index> indexesToMask_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
