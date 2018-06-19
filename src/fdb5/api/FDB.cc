/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::instance().build(config)),
    dirty_(false),
    timer_(new eckit::Timer),
    stats_(new FDBStats) {}


FDB::~FDB() {
    flush();
}

void FDB::archive(const Key& key, const void* data, size_t length) {
    timer_->start();

    internal_->archive(key, data, length);
    dirty_ = true;

    timer_->stop();
    stats_->addArchive(length, *timer_);
}

eckit::DataHandle* FDB::retrieve(const MarsRequest& request) {

    timer_->start();
    eckit::DataHandle* dh = internal_->retrieve(request);
    timer_->stop();
    stats_->addRetrieve(dh->estimate(), *timer_);

    return dh;
}

const std::string FDB::id() const {
    return internal_->id();
}

void FDB::print(std::ostream& s) const {
    s << *internal_;
}

void FDB::flush() {
    if (dirty_) {
        timer_->start();

        internal_->flush();
        dirty_ = false;

        timer_->stop();
        stats_->addFlush(*timer_);
    }
}

bool FDB::dirty() const {
    return dirty_;
}

void FDB::disable() {
    internal_->disable();
}

void FDB::reportStats(std::ostream& out) const {
    stats_->report(out, "");
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