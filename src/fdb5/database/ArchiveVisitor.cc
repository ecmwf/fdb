/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"


#include "fdb5/database/DB.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {

ArchiveVisitor::ArchiveVisitor(Archiver &owner, const Key &dataKey, const void *data, size_t size) :
    BaseArchiveVisitor(owner, dataKey),
    data_(data),
    size_(size) {
}

bool ArchiveVisitor::selectDatum(const InspectionKey &key, const Key &full) {

    // eckit::Log::info() << "selectDatum " << key << ", " << full << " " << size_ << std::endl;
    checkMissingKeys(full);

    CatalogueWriter* writeCatalogue = dynamic_cast<CatalogueWriter*>(current());
    const Index& idx = writeCatalogue->currentIndex();

    // here we could create a queue... and keep accepting archival request until the queue is full
    auto futureLocation = store()->archive(idx.key(), data_, size_);
    writeCatalogue->archive(key, futureLocation.get());

    return true;
}

void ArchiveVisitor::print(std::ostream &out) const {
    out << "ArchiveVisitor["
        << "size=" << size_
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
