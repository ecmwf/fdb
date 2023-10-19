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

BaseArchiveVisitor::BaseArchiveVisitor(Archiver &owner, const Key &dataKey) :
    WriteVisitor(owner.prev_),
    owner_(owner),
    dataKey_(dataKey) {
    checkMissingKeysOnWrite_ = eckit::Resource<bool>("checkMissingKeysOnWrite", true);
}

bool BaseArchiveVisitor::selectDatabase(const Key &dbKey, const Key&) {
    eckit::Log::debug<LibFdb5>() << "selectDatabase " << dbKey << std::endl;
    owner_.selectDatabase(dbKey);
    ASSERT(owner_.catalogue_);
    owner_.catalogue_->deselectIndex();

    return true;
}

bool BaseArchiveVisitor::selectIndex(const Key &idxKey, const Key&) {
    // eckit::Log::info() << "selectIndex " << key << std::endl;
    ASSERT(owner_.catalogue_);
    return owner_.catalogue_->selectIndex(idxKey);
}

void BaseArchiveVisitor::checkMissingKeys(const Key &full) {
    if (checkMissingKeysOnWrite_) {
        dataKey_.validateKeysOf(full);
    }
}

const Schema& BaseArchiveVisitor::databaseSchema() const {
    ASSERT(current());
    return current()->schema();
}

CatalogueWriter* BaseArchiveVisitor::current() const {
    return owner_.catalogue_;
}

Store* BaseArchiveVisitor::store() const {
    return owner_.store_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
