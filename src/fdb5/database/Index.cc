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

enum IndexBaseStreamKeys {
    IndexKeyUnrecognised,
    IndexKey,
    IndexType,
    IndexTimestamp
};

IndexBaseStreamKeys keyId(const std::string& s) {
    static const std::map<std::string, IndexBaseStreamKeys> keys {
        {"key" , IndexKey},
        {"type", IndexType},
        {"time", IndexTimestamp},
    };

    auto it = keys.find(s);
    if( it != keys.end() ) {
        return it->second;
    }
    return IndexKeyUnrecognised;
}


void IndexBase::decodeCurrent(eckit::Stream& s, const int version) {
    ASSERT(version >= 3);

    axes_.decode(s, version);

    ASSERT(s.next());
    std::string k;
    while (!s.endObjectFound()) {
        s >> k;
        switch (keyId(k)) {
            case IndexKey:
                s >> key_;
                break;
            case IndexType:
                s >> type_;
                break;
            case IndexTimestamp:
                s >> timestamp_;
                break;
            default:
                throw eckit::SeriousBug("IndexBase de-serialization error: "+k+" field is not recognized");
        }
    }
    ASSERT(!key_.empty());
    ASSERT(!type_.empty());
    ASSERT(timestamp_);
}

void IndexBase::decodeLegacy(eckit::Stream& s, const int version) { // decoding of old Stream format, for backward compatibility
    ASSERT(version <= 2);

    axes_.decode(s, version);

    std::string dummy;
    s >> key_;
    s >> dummy; ///< legacy entry, no longer used but stays here so we can read existing indexes
    s >> type_;
    timestamp_ = 0;
}


IndexBase::IndexBase(eckit::Stream& s, const int version) {
    if (version >= 3)
        decodeCurrent(s, version);
    else
        decodeLegacy(s, version);
}

IndexBase::~IndexBase() {
}

void IndexBase::encode(eckit::Stream& s, const int version) const {
    if (version >= 3) {
        encodeCurrent(s, version);
    } else {
        encodeLegacy(s, version);
    }
}

void IndexBase::encodeCurrent(eckit::Stream& s, const int version) const {
    ASSERT(version >= 3);

    axes_.encode(s, version);
    s.startObject();
    s << "key" << key_;
    s << "type" << type_;
    s << "time" << timestamp_;
    s.endObject();
}

void IndexBase::encodeLegacy(eckit::Stream& s, const int version) const {
    ASSERT(version <= 2);

    axes_.encode(s, version);
    s << key_;
    s << key_.valuesToString(); // we no longer write this field, required in the previous index format
    s << type_;
}

void IndexBase::put(const Key &key, const Field &field) {

    LOG_DEBUG_LIB(LibFdb5) << "FDB Index " << indexer_ << " " << key << " -> " << field << std::endl;

    axes_.insert(key);
    add(key, field);
}

bool IndexBase::partialMatch(const metkit::mars::MarsRequest& request) const {

    if (!key_.partialMatch(request)) return false;

    if (!axes_.partialMatch(request)) return false;

    return true;
}

bool IndexBase::mayContain(const Key &key) const {
    return axes_.contains(key);
}

bool IndexBase::mayContainPartial(const Key& key) const {
    return axes_.containsPartial(key);
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

    virtual const IndexLocation& location() const override { NOTIMP; }
//    virtual const std::vector<eckit::URI> dataUris() const { NOTIMP; }

    virtual bool dirty() const override { NOTIMP; }

    virtual void open() override { NOTIMP; }
    virtual void close() override { NOTIMP; }
    virtual void reopen() override { NOTIMP; }

    virtual void visit(IndexLocationVisitor&) const override { NOTIMP; }

    virtual bool get( const Key&, const Key&, Field&) const override { NOTIMP; }
    virtual void add( const Key&, const Field&) override { NOTIMP; }
    virtual void flush() override { NOTIMP; }
    virtual void encode(eckit::Stream&, const int version) const override { NOTIMP; }
    virtual void entries(EntryVisitor&) const override { NOTIMP; }

    virtual void print( std::ostream& s) const override { s << "NullIndex()"; }
    virtual void dump(std::ostream&, const char*, bool, bool) const override { NOTIMP; }

    virtual void flock() const override { NOTIMP; }
    virtual void funlock() const override { NOTIMP; }

    virtual IndexStats statistics() const override { NOTIMP; }

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
