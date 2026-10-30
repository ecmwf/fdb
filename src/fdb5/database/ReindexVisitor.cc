// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "fdb5/database/ReindexVisitor.h"

namespace fdb5 {

ReindexVisitor::ReindexVisitor(Reindexer& owner, const Key& initialFieldKey, const FieldLocation& fieldLocation) :
    BaseArchiveVisitor(owner, initialFieldKey), fieldLocation_(std::move(fieldLocation)) {}

bool ReindexVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {
    checkMissingKeys(fullKey);
    const Key idxKey = catalogue()->currentIndexKey();
    catalogue()->archive(idxKey, datumKey, fieldLocation_.make_shared());
    return true;
}

void ReindexVisitor::print(std::ostream& out) const {
    out << "ReindexVisitor[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
