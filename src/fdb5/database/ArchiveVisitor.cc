/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include <functional>
#include "eckit/exception/Exceptions.h"

#include "fdb5/database/Archiver.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

ArchiveVisitor::ArchiveVisitor(Archiver &owner, const Key &dataKey, const void *data, size_t size) :
    BaseArchiveVisitor(owner, dataKey),
    data_(data),
    size_(size) {
}


bool ArchiveVisitor::selectDatum(const InspectionKey &key, const Key &full) {

    checkMissingKeys(full);
    const Key idxKey = catalogue()->currentIndexKey();

    store()->archive(idxKey, data_, size_, std::bind(&CatalogueWriter::archive, catalogue(), idxKey, key, std::placeholders::_1));

    return true;
}

void ArchiveVisitor::print(std::ostream &out) const {
    out << "ArchiveVisitor["
        << "size=" << size_
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
