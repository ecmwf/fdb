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
#include "fdb5/database/FieldRef.h"

namespace fdb5 {


template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
class TBTreeIndex : public BTreeIndex {

public: // types

    typedef eckit::FixedString<KEYSIZE> BTreeKey;
    typedef eckit::BTree< BTreeKey , PAYLOAD, RECSIZE > BTreeStore;

public: // methods

    TBTreeIndex(const eckit::PathName &path, bool readOnly, off_t offset );

private: // methods

    virtual bool get(const std::string& key, FieldRef& data) const;
    virtual bool set(const std::string& key, const FieldRef& data);
    virtual void flush();
    virtual void visit(BTreeIndexVisitor& visitor) const;

private: // members

    mutable BTreeStore btree_;

};


template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::TBTreeIndex(const eckit::PathName &path, bool readOnly, off_t offset):
    btree_( path, readOnly, offset) {
}

template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
bool TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::get(const std::string& key, FieldRef &data) const {
    BTreeKey k (key);
    PAYLOAD payload;
    bool found = btree_.get(k, payload);
    if(found) {
        data = FieldRef(payload);
    }
    return found;
}


template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
bool TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::set(const std::string& key, const FieldRef &data) {
    BTreeKey k (key);
    PAYLOAD payload(data);
    return btree_.set(k, payload);
}

template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::flush() {
    btree_.flush();
}

template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
class TBTreeIndexVisitor {
    BTreeIndexVisitor &visitor_;
public:
    TBTreeIndexVisitor(BTreeIndexVisitor &visitor):
        visitor_(visitor) {}

    // BTree::range() expect a STL like collection

    void clear() {
    }

    void push_back(const typename TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::BTreeStore::result_type &kv) {
        visitor_.visit(kv.first, kv.second);
    }
};

template<int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::visit(BTreeIndexVisitor &visitor) const {
    TBTreeIndexVisitor<KEYSIZE, RECSIZE, PAYLOAD> v(visitor);
    btree_.range("", "\255", v);
}


//-----------------------------------------------------------------------------


//-----------------------------------------------------------------------------

#define BTREE(KEYSIZE, RECSIZE, PAYLOAD)                                                                   \
struct BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD : public TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD> {                  \
    BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD (const eckit::PathName& path, bool readOnly, off_t offset): \
        TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>(path, readOnly, offset){};                                  \
}; \
static IndexBuilder<BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD> \
maker_BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD("BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD")

BTREE(32, 65536, FieldRefReduced);
BTREE(32, 65536, FieldRefFull);



//-----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
}


const std::string& BTreeIndex::defaulType() {
    static std::string fdbIndexType = eckit::Resource<std::string>("fdbIndexType;$FDB_INDEX_TYPE", "BTreeIndex");
    return fdbIndexType;
}

static IndexBuilder<BTreeIndex_32_65536_FieldRefReduced> defaultIndex("BTreeIndex");


//-----------------------------------------------------------------------------

} // namespace fdb5
