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

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/daos/DaosCatalogue.h"
#include "fdb5/database/WipeVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosWipeVisitor : public WipeVisitor {

public:

    DaosWipeVisitor(const DaosCatalogue& catalogue, const metkit::mars::MarsRequest& request,
                    eckit::Queue<WipeElement>& queue, bool doit, bool porcelain, bool unsafeWipeAll);
    ~DaosWipeVisitor() override;

private:  // methods

    bool visitDatabase(const Catalogue& catalogue) override;
    bool visitIndex(const Index& index) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    void ensureSafeURIs();
    void calculateResidualURIs();

    bool anythingToWipe() const;

    void report(bool wipeAll);
    void wipe(bool wipeAll);

private:  // members

    // What are the parameters of the wipe operation
    const DaosCatalogue& catalogue_;

    metkit::mars::MarsRequest indexRequest_;

    fdb5::DaosName dbKvName_;

    std::set<fdb5::DaosKeyValueName> indexNames_;
    std::set<fdb5::DaosKeyValueName> axisNames_;
    std::set<fdb5::DaosKeyValueName> safeKvNames_;

    std::set<eckit::URI> storeURIs_;
    std::set<eckit::URI> safeStoreURIs_;

    std::set<fdb5::DaosKeyValueName> residualKvNames_;
    std::set<eckit::URI> residualStoreURIs_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
