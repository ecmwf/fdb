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

namespace fdb5 {
namespace pmem {

::eckit::ClassSpec PMemFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "PMemFieldLocation",};
::eckit::Reanimator<PMemFieldLocation> PMemFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------


PMemFieldLocation::PMemFieldLocation() :
    dataPool_(0) {
    NOTIMP; // TODO streamable
}

PMemFieldLocation::PMemFieldLocation(const PMemFieldLocation& rhs) :
    FieldLocation(rhs.length()),
    dataNode_(rhs.dataNode_),
    dataPool_(rhs.dataPool_) {}


PMemFieldLocation::PMemFieldLocation(const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPool& pool) :
    dataNode_(dataNode),
    dataPool_(&pool) {}


PMemFieldLocation::PMemFieldLocation(eckit::Stream& s) :
    FieldLocation(s),
    dataPool_(0) {
    // TODO streamable
}


std::shared_ptr<FieldLocation> PMemFieldLocation::make_shared() const {
    return std::make_shared<PMemFieldLocation>(*this);
}


eckit::DataHandle *PMemFieldLocation::dataHandle() const {

    const PDataNode& node(*dataNode_);
    return new eckit::MemoryHandle(node.data(), node.length());
}

void PMemFieldLocation::print(std::ostream &out) const {
    out << "(" << "," << length_ << ")";
}

void PMemFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

::pmem::PersistentPtr<PDataNode> PMemFieldLocation::node() const {
    return dataNode_;
}

DataPool& PMemFieldLocation::pool() const {

    // PMemFieldLocation is streamable to be able to display/store/reconstruct
    // information across RemoteFDB instances. Clearly the location cannot be
    // directly accessed on the other side.
    //
    // Without access to the DataPoolManager object, we have no way to actually
    // get the DataPool here when reconstructed from a Streamed object. If it is
    // necessary to do the reverse streaming, then this gets more complicated.

    if (!dataPool_) {
        throw eckit::SeriousBug("Attempting to access PMemFieldLocation with no pool set. Likely via Streamed object.",
                                Here());
    }
    return *dataPool_;
}

void PMemFieldLocation::encode(eckit::Stream& s) const {
    FieldLocation::encode(s);
    // TODO: Streamable
}

void PMemFieldLocation::dump(std::ostream& out) const
{
    out << "  pool_uuid: " << node().uuid() << std::endl;
    if (dataPool_) out << "  data_pool: " << dataPool_->path() << std::endl;
    out << "  offset: "    << node().offset() << std::endl;

}

eckit::PathName fdb5::pmem::PMemFieldLocation::url() const
{
    ASSERT(dataPool_);
    return dataPool_->path();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
