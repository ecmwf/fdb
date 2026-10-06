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
                               const ArchiveCallbacks& callbacks) :
    BaseArchiveVisitor(owner, initialFieldKey), data_(data), size_(size), callbacks_(callbacks) {}

std::shared_ptr<ArchiveVisitor> ArchiveVisitor::create(Archiver& owner, const Key& dataKey, const void* data,
                                                       size_t size, const ArchiveCallbacks& callbacks) {
    return std::shared_ptr<ArchiveVisitor>(new ArchiveVisitor(owner, dataKey, data, size, callbacks));
}

void ArchiveVisitor::callbacks(std::shared_ptr<CatalogueWriter> catalogue, const Key& idxKey, const Key& datumKey,
                               std::vector<std::promise<std::shared_ptr<const FieldLocation>>>& promises,
                               std::shared_ptr<const FieldLocation> fieldLocation) {
    for (auto& promise : promises) {
        promise.set_value(fieldLocation);
    }
    ASSERT(catalogue);
    catalogue->archive(idxKey, datumKey, std::move(fieldLocation));
}

bool ArchiveVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {

    checkMissingKeys(fullKey);
    const Key idxKey = catalogue()->currentIndexKey();

    // Each callback owns a future, but all futures resolve to the same location.
    auto promises =
        std::make_shared<std::vector<std::promise<std::shared_ptr<const FieldLocation>>>>(callbacks_.size());

    std::shared_ptr<ArchiveVisitor> self = shared_from_this();
    auto writer = catalogue();

    store()->archiveCb(idxKey, data_, size_,
                       [self, idxKey, datumKey, promises, writer](std::unique_ptr<const FieldLocation> loc) mutable {
                           self->callbacks(writer, idxKey, datumKey, *promises, std::move(loc));
                       });

    for (size_t i = 0; i < callbacks_.size(); ++i) {
        callbacks_[i](initialFieldKey(), data_, size_, (*promises)[i].get_future());
    }

    return true;
}

void ArchiveVisitor::print(std::ostream& out) const {
    out << "ArchiveVisitor["
        << "size=" << size_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
