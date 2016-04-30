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
#include "eckit/config/Resource.h"

#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/TocIndex.h"

namespace fdb5 {


template<int KEYSIZE, int RECSIZE>
class TBTreeIndex : public BTreeIndex {

public: // types

    typedef eckit::FixedString<KEYSIZE> BTreeKey;
    typedef FileStore::FieldRef FieldRef;
    typedef eckit::BTree< BTreeKey , FieldRef, RECSIZE > BTreeStore;

public: // methods

    TBTreeIndex(const eckit::PathName &path, bool readOnly, off_t offset );

private: // methods

    virtual bool get(const std::string& key, FileStore::FieldRef& data) const;
    virtual bool set(const std::string& key, const FileStore::FieldRef& data);
    virtual void flush();
    virtual void visit(BTreeIndexVisitor& visitor) const;

private: // members

    mutable BTreeStore btree_;

};


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


//-----------------------------------------------------------------------------

#define BTREE(KEYSIZE, RECSIZE)                                                                  \
struct BTreeIndex_##KEYSIZE##_##RECSIZE : public TBTreeIndex<KEYSIZE, RECSIZE> {                 \
    BTreeIndex_##KEYSIZE##_##RECSIZE (const eckit::PathName& path, bool readOnly, off_t offset): \
        TBTreeIndex<KEYSIZE, RECSIZE>(path, readOnly, offset){};                                 \
}


BTREE(32, 65536);
// BTREE(64, 65536);


//-----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
}


const std::string& BTreeIndex::defaulType() {
    static std::string fdbIndexType = eckit::Resource<std::string>("fdbIndexType;$FDB_INDEX_TYPE", "BTreeIndex_32_65536");
    return fdbIndexType;
}


BTreeIndex* BTreeIndex::build(const std::string& type, const eckit::PathName& path, bool readOnly, off_t offset) {
    if(type == "BTreeIndex_32_65536") {
        return new BTreeIndex_32_65536(path, readOnly, offset);
    }
    // if(type == "BTreeIndex_64_65536") {
    //     return new BTreeIndex_64_65536(path, readOnly, offset);
    // }
    throw eckit::SeriousBug("Invalid index type: " + type);
}

//-----------------------------------------------------------------------------

} // namespace fdb5
