/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemFieldLocation.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

PMemFieldLocation::PMemFieldLocation() {}


PMemFieldLocation::PMemFieldLocation(const PMemFieldLocation& rhs) :
    FieldLocation(rhs.length()),
    dataNode_(rhs.dataNode_) {}


PMemFieldLocation::PMemFieldLocation(const ::pmem::PersistentPtr< ::pmem::PersistentType<PDataNode> >& dataNode) :
    dataNode_(dataNode) {}


eckit::SharedPtr<FieldLocation> PMemFieldLocation::make_shared() const {
    return eckit::SharedPtr<FieldLocation>(new PMemFieldLocation(*this));
}


eckit::DataHandle *PMemFieldLocation::dataHandle() const {
    NOTIMP;
//    return path_.partHandle(offset_, length_);
}

void PMemFieldLocation::print(std::ostream &out) const {
    out << "(" << length_ << ")";
}

void PMemFieldLocation::visit(FieldLocationVisitor& visitor) const {
    visitor(*this);
}

::pmem::PersistentPtr< ::pmem::PersistentType<PDataNode> > PMemFieldLocation::node() const {
    return dataNode_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
