/*
 * (C) Copyright 1996-2013 ECMWF.
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

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemFieldLocation::PMemFieldLocation(const PMemFieldLocation& rhs) :
    FieldLocation(rhs.length()),
    dataNode_(rhs.dataNode_),
    dataPool_(rhs.dataPool_) {}


PMemFieldLocation::PMemFieldLocation(const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPool& pool) :
    dataNode_(dataNode),
    dataPool_(pool) {}


eckit::SharedPtr<FieldLocation> PMemFieldLocation::make_shared() const {
    return eckit::SharedPtr<FieldLocation>(new PMemFieldLocation(*this));
}


eckit::DataHandle *PMemFieldLocation::dataHandle() const {

    const PDataNode& node(*dataNode_);
    return new eckit::MemoryHandle(node.data(), node.length());
}

void PMemFieldLocation::print(std::ostream &out) const {

    out << "PMemFieldLocation(" << length_ << ")";
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

void PMemFieldLocation::dump(std::ostream& out) const
{
    out << "  pool_uuid: " << node().uuid() << std::endl;
    out << "  data_pool: " << dataPool_.path() << std::endl;
    out << "  offset: "    << node().offset() << std::endl;

}

eckit::PathName fdb5::pmem::PMemFieldLocation::url() const
{
    return dataPool_.path();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5