/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/pmem/PMemDBWriter.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


PMemDBWriter::PMemDBWriter(const Key &key) :
    PMemDB(key) {
}

PMemDBWriter::~PMemDBWriter() {
}

bool PMemDBWriter::selectIndex(const Key &key) {

    // TODO: What if it isn't there, and we are trying to read?
    // TODO: n.b. We can make this more efficient by keeping a map of the available indices.

    //if (indices_.find(key) == indices_.end()) {
    //    indices_[key] = new PMemIndex(key, root_.getBranchingNode(key));
    //}

    //currentIndex_ = indices_[key];

    currentIndex_ = &root_.getBranchingNode(key);

    return true;
}

void PMemDBWriter::print(std::ostream &out) const {
    out << "PMemDBWriter["
        /// @todo should print more here
        << "]";
}

static DBBuilder<PMemDBWriter> builder("pmem.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
