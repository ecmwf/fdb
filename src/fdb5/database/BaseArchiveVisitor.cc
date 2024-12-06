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

namespace fdb5 {

BaseArchiveVisitor::BaseArchiveVisitor(Archiver &owner, const Key& initialFieldKey) :
    WriteVisitor(owner.prev_),
    owner_(owner),
    initialFieldKey_(initialFieldKey) {
    checkMissingKeysOnWrite_ = eckit::Resource<bool>("checkMissingKeysOnWrite", true);
}

bool BaseArchiveVisitor::selectDatabase(const Key& dbKey, const Key& /* fullKey */) {
    LOG_DEBUG_LIB(LibFdb5) << "BaseArchiveVisitor::selectDatabase " << dbKey << std::endl;
    owner_.current_ = &owner_.database(dbKey);
    owner_.current_->deselectIndex();

    return true;
}

bool BaseArchiveVisitor::selectIndex(const Key& idxKey, const Key& /* fullKey */) {
    // eckit::Log::info() << "selectIndex " << idxKey << std::endl;
    ASSERT(owner_.current_);
    return owner_.current_->selectIndex(idxKey);
}

void BaseArchiveVisitor::checkMissingKeys(const Key& fullKey) const {
    if (checkMissingKeysOnWrite_) { fullKey.validateKeys(initialFieldKey_); }
}

const Schema& BaseArchiveVisitor::databaseSchema() const {
    ASSERT(current());
    return current()->schema();
}

DB* BaseArchiveVisitor::current() const {
    return owner_.current_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
