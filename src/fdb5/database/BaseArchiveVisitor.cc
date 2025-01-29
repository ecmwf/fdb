/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

BaseArchiveVisitor::BaseArchiveVisitor(Archiver &owner, const Key& initialFieldKey) :
    WriteVisitor(owner.prev_),
    owner_(owner),
    initialFieldKey_(initialFieldKey) {
    checkMissingKeysOnWrite_ = eckit::Resource<bool>("checkMissingKeysOnWrite", true);
}

bool BaseArchiveVisitor::selectDatabase(const Key& dbKey, const Key&) {
    LOG_DEBUG_LIB(LibFdb5) << "BaseArchiveVisitor::selectDatabase " << dbKey << std::endl;
    owner_.selectDatabase(dbKey);
    catalogue()->deselectIndex();

    return true;
}

bool BaseArchiveVisitor::selectIndex(const Key& idxKey, const Key&) {
    return catalogue()->selectIndex(idxKey);
}

void BaseArchiveVisitor::checkMissingKeys(const Key& fullKey) const {
    if (checkMissingKeysOnWrite_) { fullKey.validateKeys(initialFieldKey_); }
}

const Schema& BaseArchiveVisitor::databaseSchema() const {
    return catalogue()->schema();
}

CatalogueWriter* BaseArchiveVisitor::catalogue() const {
    ASSERT(owner_.db_);
    ASSERT(owner_.db_->catalogue_);
    return owner_.db_->catalogue_.get();
}

Store* BaseArchiveVisitor::store() const {
    ASSERT(owner_.db_);
    ASSERT(owner_.db_->store_);
    return owner_.db_->store_.get();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
