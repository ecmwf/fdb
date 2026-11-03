// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/io/HandleGatherer.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/MultiHandle.h"
#include "eckit/log/Plural.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

HandleGatherer::HandleGatherer(bool sorted) : sorted_(sorted), count_(0) {}

HandleGatherer::~HandleGatherer() {
    for (std::vector<eckit::DataHandle*>::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        delete (*j);
    }
}

eckit::DataHandle* HandleGatherer::dataHandle() {
    for (std::vector<eckit::DataHandle*>::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        (*j)->compress(sorted_);
    }

    eckit::DataHandle* h = new eckit::MultiHandle(handles_);
    handles_.clear();
    return h;
}

void HandleGatherer::add(eckit::DataHandle* h) {
    count_++;
    ASSERT(h);
    if (sorted_) {
        for (std::vector<eckit::DataHandle*>::iterator j = handles_.begin(); j != handles_.end(); ++j) {
            if ((*j)->merge(h)) {
                delete h;
                return;
            }
        }
    }
    else {
        if (handles_.size() > 0) {
            if (handles_.back()->merge(h)) {
                delete h;
                return;
            }
        }
    }
    handles_.push_back(h);
}

size_t HandleGatherer::count() const {
    return count_;
}

void HandleGatherer::print(std::ostream& out) const {
    out << eckit::Plural(handles_.size(), "handle");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
