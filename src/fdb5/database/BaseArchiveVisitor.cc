// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Rule.h"

namespace fdb5 {

BaseArchiveVisitor::BaseArchiveVisitor(Archiver& owner, const Key& initialFieldKey) :
    WriteVisitor(owner.prev_), owner_(owner), initialFieldKey_(initialFieldKey) {
    checkMissingKeysOnWrite_ = eckit::Resource<bool>("checkMissingKeysOnWrite", true);
}

bool BaseArchiveVisitor::selectDatabase(const Key& dbKey, const Key&) {
    LOG_DEBUG_LIB(LibFdb5) << "BaseArchiveVisitor::selectDatabase " << dbKey << std::endl;
    owner_.selectDatabase(dbKey);
    catalogue()->deselectIndex();

    return true;
}

bool BaseArchiveVisitor::selectIndex(const Key& idxKey) {
    return catalogue()->selectIndex(idxKey);
}

bool BaseArchiveVisitor::createIndex(const Key& idxKey, size_t datumKeySize) {
    return catalogue()->createIndex(idxKey, datumKeySize);
}

void BaseArchiveVisitor::checkMissingKeys(const Key& fullKey) const {
    if (checkMissingKeysOnWrite_) {
        fullKey.validateKeys(initialFieldKey_);
    }
}

const Schema& BaseArchiveVisitor::databaseSchema() const {
    return catalogue()->schema();
}

std::shared_ptr<CatalogueWriter> BaseArchiveVisitor::catalogue() const {
    ASSERT(owner_.db_);
    ASSERT(owner_.db_->catalogue_);
    return owner_.db_->catalogue_;
}

Store* BaseArchiveVisitor::store() const {
    ASSERT(owner_.db_);
    ASSERT(owner_.db_->store_);
    return owner_.db_->store_.get();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
