/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/BigNum.h"

#include "fdb5/LibFdb.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TocIndexCloser {

    const TocIndex &index_;
    bool opened_;

public:
    TocIndexCloser(const TocIndex &index): index_(index), opened_(index.btree_) {
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

/// @note We use a FileStoreWrapper base that only exists to initialise the files_ member function
///       before the Index constructor is called. This is necessary as due to (preexisting)
///       serialisation ordering, the files_ member needs to be initialised from a Stream
///       before the prefix_ and type_ members of Index, but Indexs WILL be constructed before
///       the members of TocIndex

TocIndex::TocIndex(const Key &key, const eckit::PathName &path, off_t offset, Mode mode, const std::string& type ) :
    FileStoreWrapper(path.dirName()),
    IndexBase(key, type),
    btree_( 0 ),
    dirty_(false),
    mode_(mode),
    location_(path, offset) {}

TocIndex::TocIndex(eckit::Stream &s, const eckit::PathName &directory, const eckit::PathName &path, off_t offset):
    FileStoreWrapper(directory, s),
    IndexBase(s),
    btree_(0),
    dirty_(false),
    mode_(TocIndex::READ),
    location_(path, offset) {}

TocIndex::~TocIndex() {
    eckit::Log::debug<LibFdb>() << "Closing TocIndex " << *this << std::endl;
}

void TocIndex::encode(eckit::Stream &s) const {
    files_.encode(s);
    axes_.encode(s);
    s << key_;
    s << prefix_;
    s << type_;
}


bool TocIndex::get(const Key &key, Field &field) const {
    ASSERT(btree_);
    FieldRef ref;

    bool found = btree_->get(key.valuesToString(), ref);
    if ( found ) {
        field = Field(TocFieldLocation(files_, ref), ref.details());
        // field.path_     = files_.get( ref.pathId_ );
        // field.offset_   = ref.offset_;
        // field.length_   = ref.length_;
    }
    return found;
}


void TocIndex::open() {
    if (!btree_) {
        btree_.reset(BTreeIndexFactory::build(type_, location_.path_, mode_ == TocIndex::READ, location_.offset_));
    }
}

void TocIndex::reopen() {
    close();

    // Create a new btree at the end of this one

    location_.offset_ = location_.path_.size();

    open();
}

void TocIndex::close() {
    btree_.reset(0);
}

void TocIndex::add(const Key &key, const Field &field) {
    ASSERT(btree_);
    ASSERT( mode_ == TocIndex::WRITE );

    FieldRef ref(files_, field);

    //  bool replace =
    btree_->set(key.valuesToString(), ref); // returns true if replace, false if new insert

    dirty_ = true;

}

void TocIndex::flush() {
    ASSERT( mode_ == TocIndex::WRITE );

    if (dirty_) {
        ASSERT(btree_);
        btree_->flush();
        dirty_ = false;
    }
}


void TocIndex::visit(IndexLocationVisitor &visitor) const {
    visitor(location_);
}


class TocIndexVisitor : public BTreeIndexVisitor {
    const Index& index_;
    const std::string& prefix_;
    const FileStore &files_;
    EntryVisitor &visitor_;
public:
    TocIndexVisitor(const Index& index, const std::string &prefix, const FileStore &files, EntryVisitor &visitor):
        index_(index),
        prefix_(prefix),
        files_(files),
        visitor_(visitor) {}

    void visit(const std::string& key, const FieldRef& ref) {
        Field field(TocFieldLocation(files_, ref), ref.details());
        visitor_.visit(index_, field, prefix_, key);
    }
};

void TocIndex::entries(EntryVisitor &visitor) const {
    TocIndexCloser closer(*this);

    TocIndexVisitor v( Index(const_cast<TocIndex*>(this)), prefix_, files_, visitor);
    btree_->visit(v);
}

void TocIndex::print(std::ostream &out) const {
    out << "TocIndex[path=" << location_.path_ << ",offset="<< location_.offset_ << "]";
}


std::string TocIndex::defaulType() {
    return BTreeIndex::defaulType();
}

void TocIndex::dump(std::ostream &out, const char* indent, bool simple) const {
    out << indent << "Prefix: " << prefix_ << ", key: " << key_;
    if(!simple) {
        out << std::endl;
        files_.dump(out, indent);
        axes_.dump(out, indent);
    }
}

IndexStats TocIndex::statistics() const
{
    IndexStats s(new TocIndexStats());

    NOTIMP;

    return s;
}

//-----------------------------------------------------------------------------

} // namespace fdb5
