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
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/TocIndex.h"

namespace fdb5 {


BTreeIndex::~BTreeIndex() {
}

//-----------------------------------------------------------------------------
template<int KEYSIZE, int RECSIZE>
TBTreeIndex<KEYSIZE, RECSIZE>::TBTreeIndex(const eckit::PathName &path, bool readOnly, off_t offset):
    btree_( path, readOnly, offset) {
}

template<int KEYSIZE, int RECSIZE>
bool TBTreeIndex<KEYSIZE, RECSIZE>::get(const std::string& key, FileStore::FieldRef &data) const {
    BTreeKey k (key);
    return btree_.get(k, data);
}


template<int KEYSIZE, int RECSIZE>
bool TBTreeIndex<KEYSIZE, RECSIZE>::set(const std::string& key, const FileStore::FieldRef &data) {
    BTreeKey k (key);
    return btree_.set(k, data);
}

template<int KEYSIZE, int RECSIZE>
void TBTreeIndex<KEYSIZE, RECSIZE>::flush() {
    btree_.flush();
}

template<int KEYSIZE, int RECSIZE>

class TBTreeIndexVisitor {
    BTreeIndexVisitor &visitor_;
public:
    TBTreeIndexVisitor(BTreeIndexVisitor &visitor):
        visitor_(visitor) {}

    // BTree::range() expect a STL like collection

    void clear() {
    }

    void push_back(const typename TBTreeIndex<KEYSIZE, RECSIZE>::BTreeStore::result_type &kv) {
        visitor_.visit(kv.first, kv.second);
    }
};

template<int KEYSIZE, int RECSIZE>
void TBTreeIndex<KEYSIZE, RECSIZE>::visit(BTreeIndexVisitor &visitor) const {
    TBTreeIndexVisitor<KEYSIZE, RECSIZE> v(visitor);
    btree_.range("", "\255", v);
}


BTreeIndex_32_65536::BTreeIndex_32_65536(const eckit::PathName& path, bool readOnly, off_t offset):
    TBTreeIndex<32, 65536>(path, readOnly, offset) {
}



std::string BTreeIndex::defaulType() {
    return "BTreeIndex_32_65536";
}

//-----------------------------------------------------------------------------

} // namespace fdb5
