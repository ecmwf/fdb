/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"
#include "eckit/log/BigNum.h"

#include "fdb5/Key.h"
#include "fdb5/TocIndex.h"
#include "fdb5/Error.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

TocIndex::TocIndex(const Key &key, const eckit::PathName &path, Index::Mode mode ) :
    Index(key, path, mode),
    btree_( 0 ),
    dirty_(false) {
}

TocIndex::TocIndex(eckit::Stream &s, const eckit::PathName& directory, const eckit::PathName &path):
    Index(s, directory, path),
    btree_(0),
    dirty_(false) {

}

TocIndex::~TocIndex() {
}

bool TocIndex::exists(const Key &key) const {
    ASSERT(btree_);
    BTreeKey k (key.valuesToString());
    FieldRef ignore;
    return const_cast<TocIndex *>(this)->btree_->get(k, ignore);
}

bool TocIndex::get(const Key &key, Index::Field &field) const {
    ASSERT(btree_);
    FieldRef ref;
    BTreeKey k (key.valuesToString());
    bool found = const_cast<TocIndex *>(this)->btree_->get(k, ref);
    if ( found ) {
        field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_;
        field.length_   = ref.length_;
    }
    return found;
}

TocIndex::Field TocIndex::get(const Key &key) const {
    ASSERT(btree_);
    Field result;
    FieldRef ref;
    BTreeKey k (key.valuesToString());
    eckit::Log::info() << "TocIndex get " << key << " (" << k << ")" << std::endl;
    bool found = const_cast<TocIndex *>(this)->btree_->get(k, ref);
    if ( !found ) {
        std::ostringstream oss;
        oss << "FDB key not found " << key;
        throw eckit::BadParameter(oss.str(), Here());
    }

    result.path_     = files_.get( ref.pathId_ );
    result.offset_   = ref.offset_;
    result.length_   = ref.length_;

    return result;
}


void TocIndex::open() {
    if(!btree_) {
        btree_.reset(new BTreeStore(path_, mode() == Index::READ));
    }
}

void TocIndex::close() {
    btree_.reset(0);
}

void TocIndex::put_(const Key &key, const TocIndex::Field &field) {
    ASSERT(btree_);
    ASSERT( mode() == Index::WRITE );


    BTreeKey k( key.valuesToString() );
    FieldRef ref;

    eckit::Log::info() << "TocIndex insert " << key << " (" << k << ") = " << field << std::endl;

    ref.pathId_ = files_.insert( field.path_ ); // inserts not yet in filestore
    ref.offset_ = field.offset_;
    ref.length_ = field.length_;

    bool replace = btree_->set(k, ref); // returns true if replace, false if new insert

    dirty_ = true;

}

bool TocIndex::remove(const Key &key) {
    ASSERT(btree_);
    NOTIMP;

    ASSERT( mode() == Index::WRITE );

    dirty_ = true;

    BTreeKey k( key.valuesToString() );
    return btree_->remove(k);
}

void TocIndex::flush() {
    ASSERT( mode() == Index::WRITE );

    if (dirty_) {
        ASSERT(btree_);
        btree_->flush();
        dirty_ = false;
    }

    Index::flush();
}

class BTreeIndexVisitor {
    const std::string &prefix_;
    const FileStore &files_;
    EntryVisitor &visitor_;
  public:
    BTreeIndexVisitor(const std::string &prefix, const FileStore &files, EntryVisitor &visitor):
        prefix_(prefix),
        files_(files),
        visitor_(visitor) {}

    // BTree::range() expect a STL like collection

    void clear() {

    }

    void push_back(const TocIndex::BTreeStore::result_type &kv) {
        visitor_.visit(prefix_,
                       kv.first,
                       files_.get( kv.second.pathId_ ),
                       kv.second.offset_,
                       kv.second.length_);
    }
};

void TocIndex::entries(EntryVisitor &visitor) const {
    ASSERT(btree_);
    BTreeIndexVisitor v(prefix_, files_, visitor);
    const_cast<TocIndex *>(this)->btree_->range("", "\255", v);
}

void TocIndex::deleteFiles(bool doit) const {
    ASSERT(btree_);
    eckit::Log::info() << "File to remove " << btree_->path() << std::endl;
    if (doit) {
        btree_->path().unlink();
    }

    this->Index::deleteFiles(doit);
}

void TocIndex::list(std::ostream &out) const {
    out << "TocIndex count: " << eckit::BigNum(btree_->count()) << std::endl;
}

void TocIndex::print(std::ostream &out) const {
    out << "TocIndex[]";
}


//-----------------------------------------------------------------------------

} // namespace fdb5
