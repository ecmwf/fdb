/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Index::Index(const Key& key, const std::string& type) :
    type_(type),
    axes_(),
    key_(key),
    prefix_(key.valuesToString())
{
}


Index::Index(eckit::Stream& s) :
    axes_(s),
    key_(s) {
    s >> prefix_;
    s >> type_;
}

Index::~Index() {
}


void Index::put(const Key &key, const Field &field) {    

    eckit::Log::info() << "FDB Index " << indexer_ << " " << key << " -> " << field << std::endl;

    axes_.insert(key);
    add(key, field);
}

const Key &Index::key() const {
    return key_;
}

const std::string &Index::type() const {
    return type_;
}

//----------------------------------------------------------------------------------------------------------------------


const IndexAxis &Index::axes() const {
    return axes_;
}


//----------------------------------------------------------------------------------------------------------------------


EntryVisitor::~EntryVisitor() {}

void DumpVisitor::visit(const Index& index,
                        const Field& field,
                        const std::string&,
                        const std::string& fieldFingerprint) {


    out_ << "ENTRY" << std::endl;

    fdb5::Key key(fieldFingerprint, schema_.ruleFor(dbKey_, index.key()));
    out_ << "  Key: " << dbKey_ << index.key() << key;

    FieldLocationPrinter printer(out_);
    field.location().visit(printer);

    out_ << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
