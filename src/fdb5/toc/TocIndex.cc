/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocIndex.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocIndexCloser {

    const TocIndex& index_;
    bool opened_;

public:

    TocIndexCloser(const TocIndex& index) : index_(index), opened_(index.btree_) {
        if (!opened_) {
            const_cast<TocIndex&>(index_).open();
        }
    }
    ~TocIndexCloser() {
        if (!opened_) {
            const_cast<TocIndex&>(index_).close();
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

/// @note We use a FileStoreWrapper base that only exists to initialise the uris_ member function
///       before the Index constructor is called. This is necessary as due to (preexisting)
///       serialisation ordering, the uris_ member needs to be initialised from a Stream
///       before the type_ members of Index, but Indexs WILL be constructed before
///       the members of TocIndex

TocIndex::TocIndex(const Key& key, const eckit::PathName& path, off_t offset, Mode mode, size_t datumKeySize) :
    TocIndex(key, path, offset, mode, defaultType(datumKeySize)) {}

TocIndex::TocIndex(const Key& key, const eckit::PathName& path, off_t offset, Mode mode, const std::string& type) :
    UriStoreWrapper(path.dirName()),
    IndexBase(key, type),
    btree_(nullptr),
    dirty_(false),
    mode_(mode),
    location_(path, offset),
    preloadBTree_(false) {}

TocIndex::TocIndex(eckit::Stream& s, const int version, const eckit::PathName& directory, const eckit::PathName& path,
                   off_t offset, bool preloadBTree) :
    UriStoreWrapper(directory, s),
    IndexBase(s, version),
    btree_(nullptr),
    dirty_(false),
    mode_(TocIndex::READ),
    location_(path, offset),
    preloadBTree_(preloadBTree) {}

TocIndex::~TocIndex() {
    close();
}

void TocIndex::encode(eckit::Stream& s, const int version) const {
    uris_.encode(s);
    IndexBase::encode(s, version);
}


bool TocIndex::get(const Key& key, const Key& remapKey, Field& field) const {
    ASSERT(btree_);
    FieldRef ref;

    bool found = btree_->get(key.valuesToString(), ref);
    if (found) {
        const eckit::URI& uri = uris_.get(ref.uriId());
        FieldLocation* loc =
            FieldLocationFactory::instance().build(uri.scheme(), uri, ref.offset(), ref.length(), remapKey);
        field = Field(std::move(*loc), timestamp_, ref.details());
        delete (loc);
    }
    return found;
}


void TocIndex::open() {
    if (!btree_) {
        LOG_DEBUG_LIB(LibFdb5) << "Opening " << *this << std::endl;
        btree_.reset(BTreeIndexFactory::build(type_, location_.path_, mode_ == TocIndex::READ, location_.offset_));
        if (mode_ == TocIndex::READ && preloadBTree_) {
            btree_->preload();
        }
    }
}

void TocIndex::reopen() {
    close();

    // Create a new btree at the end of this one

    location_.offset_ = location_.path_.size();

    // The axes object must be reset at this point, as the TocIndex object is no longer referring
    // to the same region in memory. (i.e. the index is still associated with the same metadata
    // at the second level of the schema, but is a NEW index).

    axes_.wipe();

    open();
}

void TocIndex::close() {
    if (btree_) {
        LOG_DEBUG_LIB(LibFdb5) << "Closing " << *this << std::endl;
        btree_.reset();
    }
}

void TocIndex::add(const Key& key, const Field& field) {
    ASSERT(btree_);
    ASSERT(mode_ == TocIndex::WRITE);

    FieldRef ref(uris_, field);

    //  bool replace =
    btree_->set(key.valuesToString(), ref);  // returns true if replace, false if new insert

    dirty_ = true;
}

void TocIndex::flush() {
    ASSERT(mode_ == TocIndex::WRITE);

    if (dirty_) {
        axes_.sort();
        ASSERT(btree_);
        btree_->flush();
        btree_->sync();
        takeTimestamp();
        dirty_ = false;
    }
}


void TocIndex::visit(IndexLocationVisitor& visitor) const {
    visitor(location_);
}

void TocIndex::flock() const {
    ASSERT(btree_);
    btree_->flock();
}
void TocIndex::funlock() const {
    ASSERT(btree_);
    btree_->funlock();
}

class TocIndexVisitor : public BTreeIndexVisitor {
    const UriStore& uris_;
    EntryVisitor& visitor_;

public:

    TocIndexVisitor(const UriStore& uris, EntryVisitor& visitor) : uris_(uris), visitor_(visitor) {}

    void visit(const std::string& keyFingerprint, const FieldRef& ref) {

        Field field(TocFieldLocation(uris_, ref), visitor_.indexTimestamp(), ref.details());
        visitor_.visitDatum(field, keyFingerprint);
    }
};

void TocIndex::entries(EntryVisitor& visitor) const {

    Index instantIndex(const_cast<TocIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {
        TocIndexCloser closer(*this);
        TocIndexVisitor v(uris_, visitor);
        btree_->visit(v);
    }
}

void TocIndex::print(std::ostream& out) const {
    out << "TocIndex(path=" << location_.path_ << ",offset=" << location_.offset_ << ")";
}

const std::string& TocIndex::defaultType(size_t keySize) {
    return BTreeIndex::defaultType(keySize);
}

std::vector<eckit::URI> TocIndex::dataURIs() const {
    return uris_.paths();
}

bool TocIndex::dirty() const {
    return dirty_;
}


class DumpBTreeVisitor : public BTreeIndexVisitor {
    std::ostream& out_;
    std::string indent_;

public:

    DumpBTreeVisitor(std::ostream& out, const std::string& indent) : out_(out), indent_(indent) {}
    virtual ~DumpBTreeVisitor() {}

    void visit(const std::string& key, const FieldRef& ref) {

        out_ << indent_ << "Fingerprint: " << key << ", location: " << ref << std::endl;
    }
};


void TocIndex::dump(std::ostream& out, const char* indent, bool simple, bool dumpFields) const {
    out << indent << "Key: " << key_;

    if (!simple) {
        out << std::endl;
        uris_.dump(out, indent);
        axes_.dump(out, indent);
    }

    if (dumpFields) {
        DumpBTreeVisitor v(out, std::string(indent) + std::string("  "));

        out << std::endl;
        out << indent << "Contents of index: " << std::endl;

        TocIndexCloser closer(*this);
        btree_->visit(v);
    }
}

IndexStats TocIndex::statistics() const {
    IndexStats s(new TocIndexStats());
    return s;
}

//-----------------------------------------------------------------------------

}  // namespace fdb5
