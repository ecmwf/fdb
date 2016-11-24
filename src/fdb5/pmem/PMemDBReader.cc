/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/pmem/PMemDBReader.h"
#include "fdb5/pmem/PMemIndex.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemDBReader::PMemDBReader(const Key &key) :
    PMemDB(key) {}

PMemDBReader::~PMemDBReader() {
}


bool PMemDBReader::selectIndex(const Key &key) {

    if (indexes_.find(key) == indexes_.end()) {

        ::pmem::PersistentPtr<PBranchingNode> node = root_.getBranchingNode(key);
        if (!node.null())
            indexes_[key] = new PMemIndex(key, *node, dataPoolMgr_);
        else
            return false;
    }

    currentIndex_ = indexes_[key];

    eckit::Log::info() << "PMemDBReader::selectIndex " << key
                       << ", found match" << std::endl;

    return true;
}


void PMemDBReader::axis(const std::string& keyword, eckit::StringSet& s) const {

    ASSERT(currentIndex_);
    s = currentIndex_->axes().values(keyword);
}


eckit::DataHandle* PMemDBReader::retrieve(const Key &key) const {

    eckit::Log::info() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::info() << "From index " << currentIndex_ << std::endl;

    Field field;
    if (currentIndex_->get(key, field))
        return field.dataHandle();

    return 0;
}


void PMemDBReader::print(std::ostream &out) const {
    out << "PMemDBReader["
        /// @todo should print more here
        << "]";
}

static DBBuilder<PMemDBReader> builder("pmem.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
