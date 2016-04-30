/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"

#include "fdb5/database/Archiver.h"
#include "fdb5/database/BaseArchiveVisitor.h"
#include "fdb5/rules/Rule.h"

namespace fdb5 {

BaseArchiveVisitor::BaseArchiveVisitor(Archiver &owner, const Key &field) :
    WriteVisitor(owner.prev_),
    owner_(owner),
    field_(field) {
    checkMissingKeysOnWrite_ = eckit::Resource<bool>("checkMissingKeysOnWrite", true);
}

bool BaseArchiveVisitor::selectDatabase(const Key &key, const Key &full) {
    eckit::Log::info() << "selectDatabase " << key << std::endl;
    owner_.current_ = &owner_.database(key);
    owner_.current_->checkSchema(key);

    owner_.current_->deselectIndex();

    return true;
}

bool BaseArchiveVisitor::selectIndex(const Key &key, const Key &full) {
    // eckit::Log::info() << "selectIndex " << key << std::endl;
    ASSERT(owner_.current_);
    return owner_.current_->selectIndex(key);
}

void BaseArchiveVisitor::checkMissingKeys(const Key &full) {
    if (checkMissingKeysOnWrite_) {
        eckit::StringSet missing;

        for (Key::const_iterator j = field_.begin(); j != field_.end(); ++j) {
            if (full.find((*j).first) == full.end()) {
                missing.insert((*j).first);
            }
        }

        if (missing.size()) {
            std::ostringstream oss;
            oss << "Keys not used in archiving: " << missing;

            if (rule()) {
                oss << " " << *rule();
            }

            throw eckit::SeriousBug(oss.str());
        }
    }
}

DB *BaseArchiveVisitor::current() const {
    return owner_.current_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
