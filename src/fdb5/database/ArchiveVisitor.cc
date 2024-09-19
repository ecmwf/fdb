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

ArchiveVisitor::ArchiveVisitor(Archiver& owner, const Key& initialFieldKey, const void *data, size_t size, const ArchiveCallback& callback) :
    BaseArchiveVisitor(owner, initialFieldKey),
    data_(data),
    size_(size),
    callback_(callback){
}

void ArchiveVisitor::callbacks(fdb5::CatalogueWriter* catalogue, const Key& idxKey, const Key& datumKey, std::shared_ptr<std::promise<std::shared_ptr<const FieldLocation>>> p, std::shared_ptr<const FieldLocation> fieldLocation) {
    p->set_value(fieldLocation);
    catalogue->archive(idxKey, datumKey, std::move(fieldLocation));
}

bool ArchiveVisitor::selectDatum(const TypedKey& datumKey, const TypedKey& fullComputedKey) {

    checkMissingKeys(fullComputedKey);
    const Key idxKey = catalogue()->currentIndexKey();

    std::shared_ptr<std::promise<std::shared_ptr<const FieldLocation>>> p = std::make_shared<std::promise<std::shared_ptr<const FieldLocation>>>(std::promise<std::shared_ptr<const FieldLocation>>());

    store()->archive(idxKey, data_, size_,
        std::bind(&ArchiveVisitor::callbacks, this, catalogue(), idxKey, datumKey.canonical(), p, std::placeholders::_1));
    callback_(initialFieldKey_, data_, size_, p->get_future());

    return true;
}

void ArchiveVisitor::print(std::ostream &out) const {
    out << "ArchiveVisitor["
        << "size=" << size_
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
