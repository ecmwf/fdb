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

ArchiveVisitor::ArchiveVisitor(Archiver& owner, const Key& initialFieldKey, const void *data, size_t size, const ArchiveCallback& callback) :
    BaseArchiveVisitor(owner, initialFieldKey),
    data_(data),
    size_(size),
    callback_(callback){
}

bool ArchiveVisitor::selectDatum(const TypedKey& datumKey, const TypedKey& fullComputedKey) {

    checkMissingKeys(fullComputedKey);

    ASSERT(current());

    current()->archive(datumKey.canonical(), data_, size_, field_, callback_);

    return true;
}

void ArchiveVisitor::print(std::ostream &out) const {
    out << "ArchiveVisitor["
        << "size=" << size_
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
