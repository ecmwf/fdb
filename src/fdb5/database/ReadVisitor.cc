/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/log/Log.h"

#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/Store.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ReadVisitor::~ReadVisitor() {
}

CatalogueReader* ReadVisitor::reader() const {
    if (catalogue_) {
        CatalogueReader* catalogueReader = dynamic_cast<CatalogueReader*>(catalogue_);
        ASSERT(catalogueReader);
        return catalogueReader;
    }
    return nullptr;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
