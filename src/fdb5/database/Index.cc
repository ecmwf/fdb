/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Index.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexBase::IndexBase(const Key& key, const std::string& type) : type_(type), key_(key) {}

enum IndexBaseStreamKeys {
    IndexKeyUnrecognised,
    IndexKey,
    IndexType,
    IndexTimestamp
};

IndexBaseStreamKeys keyId(const std::string& s) {
    static const std::map<std::string, IndexBaseStreamKeys> keys{
        {"key", IndexKey},
        {"type", IndexType},
        {"time", IndexTimestamp},
    };

    auto it = keys.find(s);
    if (it != keys.end()) {
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
                throw eckit::SeriousBug("IndexBase de-serialization error: " + k + " field is not recognized");
        }
    }
    ASSERT(!key_.empty());
    ASSERT(!type_.empty());
    ASSERT(timestamp_);
}

void IndexBase::decodeLegacy(eckit::Stream& s,
                             const int version) {  // decoding of old Stream format, for backward compatibility
    ASSERT(version <= 2);

    axes_.decode(s, version);


    std::string dummy;
    s >> key_;
    s >> dummy;  ///< legacy entry, no longer used but stays here so we can read existing indexes
    s >> type_;
    timestamp_ = 0;
}

IndexBase::IndexBase(eckit::Stream& s, const int version) {
    if (version >= 3) {
        decodeCurrent(s, version);
    }
    else {
        decodeLegacy(s, version);
    }
}

IndexBase::~IndexBase() {}

void IndexBase::encode(eckit::Stream& s, const int version) const {
    if (version >= 3) {
        encodeCurrent(s, version);
    }
    else {
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
    s << "";  // we no longer write this field, required in the previous index format
    s << type_;
}

void IndexBase::put(const Key& key, const Field& field) {

    LOG_DEBUG_LIB(LibFdb5) << "FDB Index " << indexer_ << " " << key << " -> " << field << std::endl;

    axes_.insert(key);
    add(key, field);
}

bool IndexBase::partialMatch(const metkit::mars::MarsRequest& indexRequest,
                             const metkit::mars::MarsRequest& datumRequest) const {

    if (!key_.partialMatch(indexRequest)) {
        return false;
    }

    return axes_.partialMatch(datumRequest);
}

bool IndexBase::mayContain(const Key& key) const {
    return axes_.contains(key);
}

bool IndexBase::mayContainPartial(const Key& key) const {
    return axes_.containsPartial(key);
}

const Key& IndexBase::key() const {
    return key_;
}

const std::string& IndexBase::type() const {
    return type_;
}

const IndexAxis& IndexBase::axes() const {
    return axes_;
}

//----------------------------------------------------------------------------------------------------------------------
class NullIndex : public IndexBase {

public:  // methods

    NullIndex() : IndexBase(Key{}, "null") {}

private:  // methods

    const IndexLocation& location() const override { NOTIMP; }

    bool dirty() const override { NOTIMP; }

    void open() override { NOTIMP; }
    void close() override { NOTIMP; }
    void reopen() override { NOTIMP; }

    void visit(IndexLocationVisitor&) const override { NOTIMP; }

    bool get(const Key&, const Key&, Field&) const override { NOTIMP; }
    void add(const Key&, const Field&) override { NOTIMP; }
    void flush() override { NOTIMP; }
    void encode(eckit::Stream&, const int version) const override { NOTIMP; }
    void entries(EntryVisitor&) const override { NOTIMP; }

    void print(std::ostream& s) const override { s << "NullIndex()"; }
    void dump(std::ostream&, const char*, bool, bool) const override { NOTIMP; }

    void flock() const override { NOTIMP; }
    void funlock() const override { NOTIMP; }

    IndexStats statistics() const override { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------

Index::Index() : content_(new NullIndex()), null_(true) {
    content_->attach();
}

Index::Index(IndexBase* p) : content_(p), null_(false) {
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
    null_ = s.null_;
    content_->attach();
    return *this;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
