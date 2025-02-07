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
#include "fdb5/toc/TocCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocWipeVisitor : public WipeVisitor {

public:

    TocWipeVisitor(const TocCatalogue& catalogue, const Store& store, const metkit::mars::MarsRequest& request,
                   std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll);
    ~TocWipeVisitor() override;

private:  // methods

    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index& index) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    void addMaskedPaths();
    void addMetadataPaths();
    void ensureSafePaths();
    void calculateResidualPaths();
    std::vector<eckit::PathName> getAuxiliaryPaths(const eckit::URI& uri);

    bool anythingToWipe() const;

    void report(bool wipeAll);
    void wipe(bool wipeAll);

private:  // members

    // What are the parameters of the wipe operation
    const TocCatalogue& catalogue_;
    const Store& store_;

    metkit::mars::MarsRequest indexRequest_;

    std::string owner_;

    eckit::PathName tocPath_;
    eckit::PathName schemaPath_;

    std::set<eckit::PathName> subtocPaths_;
    std::set<eckit::PathName> lockfilePaths_;
    std::set<eckit::PathName> indexPaths_;
    std::set<eckit::PathName> dataPaths_;
    std::set<eckit::PathName> auxiliaryDataPaths_;

    std::set<eckit::PathName> safePaths_;
    std::set<eckit::PathName> residualPaths_;
    std::set<eckit::PathName> residualDataPaths_;

    std::vector<Index> indexesToMask_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
