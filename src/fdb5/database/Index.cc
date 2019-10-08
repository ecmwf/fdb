/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/database/EntryVisitMechanism.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexBase::IndexBase(const Key& key, const std::string& type) :
    type_(type),
    axes_(),
    key_(key)
{
}


IndexBase::IndexBase(eckit::Stream& s) :
    axes_(s),
    key_(s) {
    std::string dummy;
    s >> dummy; ///< legacy entry, no longer used but stays here so we can read existing indexes
    s >> type_;
}

IndexBase::~IndexBase() {
}

void IndexBase::put(const Key &key, const Field &field) {

    eckit::Log::debug<LibFdb5>() << "FDB Index " << indexer_ << " " << key << " -> " << field << std::endl;

    axes_.insert(key);
    add(key, field);
}

bool IndexBase::mayContain(const Key &key) const {
    return axes_.contains(key);
}

const Key &IndexBase::key() const {
    return key_;
}

const std::string &IndexBase::type() const {
    return type_;
}

const IndexAxis &IndexBase::axes() const {
    return axes_;
}


//----------------------------------------------------------------------------------------------------------------------


// TODO: Remove/convert to other visitor type
/*void DumpVisitor::visit(const Index& index,
                        const Field& field,
                        const std::string&,
                        const std::string& fieldFingerprint) {


    out_ << "ENTRY" << std::endl;

    fdb5::Key key(fieldFingerprint, schema_.ruleFor(dbKey_, index.key()));
    out_ << "  Key: " << dbKey_ << index.key() << key;

    FieldLocationPrinter printer(out_);
    field.location().visit(printer);

    out_ << std::endl;
}*/

//----------------------------------------------------------------------------------------------------------------------

class NullIndex : public IndexBase {

public: // methods

    NullIndex() : IndexBase(Key(), "null") {}

private: // methods

    virtual const IndexLocation& location() const { NOTIMP; }
    virtual const std::vector<eckit::PathName> dataPaths() const { NOTIMP; }

    virtual bool dirty() const { NOTIMP; }

    virtual void open()  { NOTIMP; }
    virtual void close() { NOTIMP; }
    virtual void reopen() { NOTIMP; }

    virtual void visit(IndexLocationVisitor&) const  { NOTIMP; }

    virtual bool get( const Key&, Field&) const  { NOTIMP; }
    virtual void add( const Key&, const Field&)  { NOTIMP; }
    virtual void flush()  { NOTIMP; }
    virtual void encode(eckit::Stream&) const { NOTIMP; }
    virtual void entries(EntryVisitor&) const { NOTIMP; }

    virtual void print( std::ostream& s) const  { s << "NullIndex()"; }
    virtual void dump(std::ostream&, const char*, bool, bool) const  { NOTIMP; }

    virtual IndexStats statistics() const { NOTIMP; }

};

//----------------------------------------------------------------------------------------------------------------------

Index::Index() :
    content_(new NullIndex()),
    null_(true) {
    content_->attach();
}

Index::Index(IndexBase* p) :
    content_(p),
    null_(false) {
    ASSERT(p);
    content_->attach();
}

Index::~Index() {
   content_->detach();
}

Index::Index(const Index& s) : content_(s.content_), null_(s.null_) {
    content_->attach();
}

Index& Index::operator=(const Index& s) {
    content_->detach();
    content_ = s.content_;
    null_    = s.null_;
    content_->attach();
    return *this;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
