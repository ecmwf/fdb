/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Store.h"

#include <functional>

namespace fdb5 {

ArchiveVisitor::ArchiveVisitor(Archiver& owner, const Key& initialFieldKey, const void* data, size_t size,
                               const ArchiveCallback& callback) :
    BaseArchiveVisitor(owner, initialFieldKey), data_(data), size_(size), callback_(callback) {}

std::shared_ptr<ArchiveVisitor> ArchiveVisitor::create(Archiver& owner, const Key& dataKey, const void* data,
                                                       size_t size, const ArchiveCallback& callback) {
    return std::shared_ptr<ArchiveVisitor>(new ArchiveVisitor(owner, dataKey, data, size, callback));
}

void ArchiveVisitor::callbacks(std::shared_ptr<CatalogueWriter> catalogue, const Key& idxKey, const Key& datumKey,
                               std::shared_ptr<std::promise<std::shared_ptr<const FieldLocation>>> p,
                               std::shared_ptr<const FieldLocation> fieldLocation) {
    p->set_value(fieldLocation);
    ASSERT(catalogue);
    catalogue->archive(idxKey, datumKey, std::move(fieldLocation));
}

bool ArchiveVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {

    checkMissingKeys(fullKey);
    const Key idxKey = catalogue()->currentIndexKey();

    auto p = std::make_shared<std::promise<std::shared_ptr<const FieldLocation>>>();

    std::shared_ptr<ArchiveVisitor> self = shared_from_this();
    auto writer = catalogue();

    store()->archiveCb(idxKey, data_, size_,
                       [self, idxKey, datumKey, p, writer](std::unique_ptr<const FieldLocation> loc) mutable {
                           self->callbacks(writer, idxKey, datumKey, p, std::move(loc));
                       });

    callback_(initialFieldKey(), data_, size_, p->get_future());

    return true;
}

void ArchiveVisitor::print(std::ostream& out) const {
    out << "ArchiveVisitor["
        << "size=" << size_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
