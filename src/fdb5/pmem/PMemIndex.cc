/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/Buffer.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/MemoryBufferStream.h"

using namespace eckit;


namespace fdb5 {
namespace pmem {

//-----------------------------------------------------------------------------


PMemIndex::PMemIndex(const Key &key, PBranchingNode& node, DataPoolManager& mgr, const std::string& type) :
    Index(key, type),
    location_(node, mgr) {

    if (!location_.node().axis_.null()) {

        Log::error() << "** Loading axes from buffer" << std::endl;
        const ::pmem::PersistentBuffer& buf(*location_.node().axis_);
        MemoryStream s(buf.data(), buf.size());
        axes_.decode(s);
        Log::error() << "axes_ " << axes_ << std::endl;
    }
}


PMemIndex::~PMemIndex() {}


void PMemIndex::visitLocation(IndexLocationVisitor& visitor) const {
    visitor(location_);
}


bool PMemIndex::get(const Key &key, Field &field) const {

    ::pmem::PersistentPtr<PDataNode> node = location_.node().getDataNode(key, location_.pool_manager());

    if (!node.null()) {
        field = Field(PMemFieldLocation(node));
        return true;
    }

    return false;
}


void PMemIndex::open() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::reopen() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::close() {
    // Intentionally left blank. Indices neither opened nor closed (part of open DB).
}

void PMemIndex::add(const Key &key, const Field &field) {

    struct Inserter : FieldLocationVisitor {
        Inserter(PBranchingNode& indexNode, const Key& key, DataPoolManager& poolManager) :
            indexNode_(indexNode),
            key_(key),
            poolManager_(poolManager) {}

        virtual void operator() (const PMemFieldLocation& location) {
            indexNode_.insertDataNode(key_, location.node(), poolManager_);
        }

    private:
        PBranchingNode& indexNode_;
        const Key& key_;
        DataPoolManager& poolManager_;
    };

    Inserter inserter(location_.node(), key, location_.pool_manager());
    field.location().visit(inserter);

    // Keep track of the axes()

    if (axes().dirty()) {

        MemoryBufferStream s;
        axes().encode(s);

        const void* data = s.buffer();
        if (location_.node().axis_.null())
            location_.node().axis_.allocate(data, s.position());
        else
            location_.node().axis_.replace(data, s.position());

        axes_.clean();
    }
}

void PMemIndex::flush() {
    // Intentionally left blank. Flush not used in PMem case.
}

void PMemIndex::encode(Stream& s) const {
    NOTIMP;
}

void PMemIndex::entries(EntryVisitor &visitor) const {
    NOTIMP;
}

void PMemIndex::print(std::ostream &out) const {
    out << "PMemIndex[]";
}


std::string PMemIndex::defaulType() {
    return "PMemIndex";
}

void PMemIndex::dump(std::ostream& out, const char* indent, bool simple) const {
    NOTIMP;
}


//-----------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
