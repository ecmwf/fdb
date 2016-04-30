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
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/BTreeIndex.h"

namespace fdb5 {


class TocIndexCloser {
    bool opened_;
    const TocIndex &index_;
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

//-----------------------------------------------------------------------------

TocIndex::TocIndex(const Key &key, const eckit::PathName &path, off_t offset, Index::Mode mode ) :
    Index(key, path, offset, mode),
    btree_( 0 ),
    dirty_(false) {
}

TocIndex::TocIndex(eckit::Stream &s, const eckit::PathName &directory, const eckit::PathName &path, off_t offset):
    Index(s, directory, path, offset),
    btree_(0),
    dirty_(false) {

}

TocIndex::~TocIndex() {
}

bool TocIndex::get(const Key &key, Index::Field &field) const {
    ASSERT(btree_);
    FileStore::FieldRef ref;

    bool found = btree_->get(key.valuesToString(), ref);
    if ( found ) {
        field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_;
        field.length_   = ref.length_;
    }
    return found;
}


void TocIndex::open() {
    if (!btree_) {
        btree_.reset(new BTreeIndex_32_65536(path_, mode_ == Index::READ, offset_));
    }
}

void TocIndex::reopen() {
    close();

    // Create a new btree at the end of this one

    offset_ = path_.size();

    open();
}

void TocIndex::close() {
    btree_.reset(0);
}

void TocIndex::add(const Key &key, const TocIndex::Field &field) {
    ASSERT(btree_);
    ASSERT( mode_ == Index::WRITE );

    FileStore::FieldRef ref;

    // eckit::Log::info() << "TocIndex insert " << key << " (" << k << ") = " << field << std::endl;

    ref.pathId_ = files_.insert( field.path_ ); // inserts not yet in filestore
    ref.offset_ = field.offset_;
    ref.length_ = field.length_;

    //  bool replace =
    btree_->set(key.valuesToString(), ref); // returns true if replace, false if new insert

    dirty_ = true;

}

void TocIndex::flush() {
    ASSERT( mode_ == Index::WRITE );

    if (dirty_) {
        ASSERT(btree_);
        btree_->flush();
        dirty_ = false;
    }
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

    void visit(const std::string& key, const FileStore::FieldRef& field) {
        visitor_.visit(index_,
                       prefix_,
                       key,
                       files_.get( field.pathId_ ),
                       field.offset_,
                       field.length_);
    }
};

void TocIndex::entries(EntryVisitor &visitor) const {
    TocIndexCloser closer(*this);

    TocIndexVisitor v(*this, prefix_, files_, visitor);
    btree_->visit(v);
}

void TocIndex::print(std::ostream &out) const {
    out << "TocIndex[path=" << path_ << ",offset="<< offset_ << "]";
}


//-----------------------------------------------------------------------------

} // namespace fdb5
