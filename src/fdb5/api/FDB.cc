/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::instance().build(config)),
    dirty_(false) {}


FDB::~FDB() {
    flush();
}

void FDB::archive(const Key& key, const void* data, size_t length) {
    internal_->archive(key, data, length);
    dirty_ = true;
}

eckit::DataHandle *FDB::retrieve(const MarsRequest& request) {
    // TODO: Match?
    return internal_->retrieve(request);
}

const std::string FDB::id() const {
    return internal_->id();
}

void FDB::print(std::ostream& s) const {
    s << *internal_;
}

void FDB::flush() {
    if (dirty_) {
        internal_->flush();
        dirty_ = false;
    }
}

bool FDB::dirty() const {
    return dirty_;
}

void FDB::disable() {
    internal_->disable();
}

bool FDB::disabled() const {
    return internal_->disabled();
}

bool FDB::writable() const {
    return internal_->writable();
}

bool FDB::visitable() const {
    return internal_->visitable();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
