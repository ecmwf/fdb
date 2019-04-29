/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/MemoryHandle.h"

#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/PDataNode.h"
#include "fdb5/pmem/DataPool.h"
#include "fdb5/toc/TocFieldLocation.h"

namespace fdb5 {
namespace pmem {

// PMemFieldLocation cannot be sensibly reconstructed on a remote.
// Create something that gives info without needing the pmem library, that
// could be remapped into a PMemFieldLocation if we later so chose.

// --> For info purposes we return a TocFieldLocation which has the required
//     components.
// --> Obviously, if this needs to be reconstructed, then we need to do
//     something else magical.

//::eckit::ClassSpec PMemFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "PMemFieldLocation",};
//::eckit::Reanimator<PMemFieldLocation> PMemFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


PMemFieldLocation::PMemFieldLocation(const PMemFieldLocation& rhs) :
    FieldLocation(rhs.length()),
    dataNode_(rhs.dataNode_),
    dataPool_(rhs.dataPool_) {}


PMemFieldLocation::PMemFieldLocation(const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPool& pool) :
    FieldLocation(dataNode->length()),
    dataNode_(dataNode),
    dataPool_(pool) {}


std::shared_ptr<FieldLocation> PMemFieldLocation::make_shared() const {
    return std::make_shared<PMemFieldLocation>(*this);
}

std::shared_ptr<FieldLocation> PMemFieldLocation::stableLocation() const {
    return std::make_shared<TocFieldLocation>(url(), node().offset(), node()->length());
}


eckit::DataHandle *PMemFieldLocation::dataHandle(const Key& remapKey) const {

    if (!remapKey.empty()) {
        throw eckit::NotImplemented("fdb-mount functionality not implemented in pmem backend (yet)", Here());
    }

    const PDataNode& node(*dataNode_);
    return new eckit::MemoryHandle(node.data(), node.length());
}

void PMemFieldLocation::print(std::ostream& out) const {
    out << "(" << node().uuid() << "," << node().offset() << "," << length_ << ")";
}

void PMemFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

::pmem::PersistentPtr<PDataNode> PMemFieldLocation::node() const {
    return dataNode_;
}

DataPool& PMemFieldLocation::pool() const {
    return dataPool_;
}

void PMemFieldLocation::encode(eckit::Stream& s) const {
    NOTIMP; // See comment
}

void PMemFieldLocation::dump(std::ostream& out) const
{
    out << "  pool_uuid: " << node().uuid() << std::endl;
    out << "  data_pool: " << pool().path() << std::endl;
    out << "  offset: "    << node().offset() << std::endl;

}

eckit::PathName fdb5::pmem::PMemFieldLocation::url() const {
    return pool().path();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
