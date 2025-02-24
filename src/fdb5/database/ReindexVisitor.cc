/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "fdb5/database/ReindexVisitor.h"

namespace fdb5 {

ReindexVisitor::ReindexVisitor(Reindexer& owner, const Key& initialFieldKey, const FieldLocation& fieldLocation) :
    BaseArchiveVisitor(owner, initialFieldKey), fieldLocation_(std::move(fieldLocation)) {}

bool ReindexVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {
    checkMissingKeys(fullKey);
    const Key idxKey = catalogue()->currentIndexKey();
    catalogue()->archive(idxKey, datumKey, fieldLocation_.shared_from_this());
    return true;
}

void ReindexVisitor::print(std::ostream& out) const {
    out << "ReindexVisitor[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
