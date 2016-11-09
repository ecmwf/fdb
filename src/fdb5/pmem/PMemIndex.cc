/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PMemFieldLocation.h"

namespace fdb5 {
namespace pmem {


//-----------------------------------------------------------------------------


PMemIndex::PMemIndex(const Key &key, PBranchingNode& node, const std::string& type) :
    Index(key, type),
    location_(node) {}


PMemIndex::~PMemIndex() {}


void PMemIndex::visitLocation(IndexLocationVisitor& visitor) const {
    visitor(location_);
}


bool PMemIndex::get(const Key &key, Field &field) const {
    NOTIMP;
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
        Inserter(PBranchingNode& indexNode, const Key& key) :
            indexNode_(indexNode),
            key_(key) {}

        virtual void operator() (const PMemFieldLocation& location) {
            indexNode_.insertDataNode(key_, location.node());
        }

    private:
        PBranchingNode& indexNode_;
        const Key& key_;
    };

    Inserter inserter(location_.node(), key);
    field.location().visit(inserter);
}

void PMemIndex::flush() {
    // Intentionally left blank. Flush not used in PMem case.
}

void PMemIndex::encode(eckit::Stream& s) const {
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
