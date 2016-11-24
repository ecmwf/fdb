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
#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/PDataNode.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemDBWriter::PMemDBWriter(const Key &key) :
    PMemDB(key) {}

PMemDBWriter::~PMemDBWriter() {
}

bool PMemDBWriter::selectIndex(const Key &key) {

    if (indexes_.find(key) == indexes_.end()) {
        indexes_[key] = new PMemIndex(key, root_.getCreateBranchingNode(key), dataPoolMgr_);
    }

    currentIndex_ = indexes_[key];

    eckit::Log::info() << "PMemDBWriter::selectIndex " << key
                       << ", found match" << std::endl;

    return true;
}

void PMemDBWriter::close() {

    // Close any open indices

    for (IndexStore::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
        Index* idx = it->second;
        idx->close();
        delete idx;
    }
}

void PMemDBWriter::deselectIndex() {
    // This is essentially a NOP, as we don't have any files to open, etc.
    currentIndex_ = 0;
}

void PMemDBWriter::archive(const Key &key, const void *data, Length length) {

    ASSERT(currentIndex_ != 0);

    // Get the key:value identifier associated with this key

    std::string data_key = key.names().back();
    std::string data_value = key.value(data_key);

    // Note that this pointer is NOT inside persistent space. The craeated object will be
    // orphaned if anything goes wrong before it is added to a tree structure.

    ::pmem::PersistentPtr<PDataNode> ptr;
    dataPoolMgr_.allocate(ptr, PDataNode::Constructor(data_key, data_value, data, length));

    Field field( (PMemFieldLocation(ptr)) );

    currentIndex_->put(key, field);
}

void PMemDBWriter::print(std::ostream &out) const {
    out << "PMemDBWriter["
        /// @todo should print more here
        << "]";
}

static DBBuilder<PMemDBWriter> builder("pmem.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
