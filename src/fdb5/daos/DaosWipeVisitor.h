/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date May 2023

#pragma once

#include "fdb5/database/WipeVisitor.h"
#include "fdb5/daos/DaosCatalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosWipeVisitor : public WipeVisitor {

public:

    DaosWipeVisitor(const DaosCatalogue& catalogue,
                    const Store& store,
                    const metkit::mars::MarsRequest& request,
                    std::ostream& out,
                    bool doit,
                    bool porcelain,
                    bool unsafeWipeAll);
    ~DaosWipeVisitor() override;

private: // methods

    bool visitDatabase(const Catalogue& catalogue, const Store& store) override;
    bool visitIndex(const Index& index) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    // void addMaskedPaths();
    // void addMetadataPaths();
    void ensureSafePaths();
    void calculateResidualPaths();

    bool anythingToWipe() const;

    void report(bool wipeAll);
    void wipe(bool wipeAll);

private: // members

    // What are the parameters of the wipe operation
    const DaosCatalogue& catalogue_;
    const Store& store_;

    metkit::mars::MarsRequest indexRequest_;

    // std::string owner_;

    // eckit::PathName tocPath_;
    fdb5::DaosName dbKvPath_;
    // eckit::PathName schemaPath_;

    // std::set<eckit::PathName> subtocPaths_;
    // std::set<eckit::PathName> lockfilePaths_;
    std::set<fdb5::DaosKeyValueName> indexPaths_;
    std::set<fdb5::DaosKeyValueName> axisPaths_;
    std::set<fdb5::DaosKeyValueName> safeKvPaths_;

    std::set<eckit::URI> storePaths_;
    std::set<eckit::URI> safeStorePaths_;
    
    std::set<fdb5::DaosKeyValueName> residualKvPaths_;
    std::set<eckit::URI> residualStorePaths_;

    // std::vector<Index> indexesToMask_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
