/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/WriteVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WriteVisitor::WriteVisitor(std::vector<Key> &prev) :
    prev_(prev),
    rule_(0) {
    prev.resize(3);
}

WriteVisitor::~WriteVisitor() {
}

void WriteVisitor::resetPreviousVisitedKey() {
    for (auto& k : prev_) {
        k = Key();
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
