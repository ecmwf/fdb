/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/log/BigNum.h"

#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

BTreeIndexVisitor::~BTreeIndexVisitor() {}

//----------------------------------------------------------------------------------------------------------------------

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
class TBTreeIndex : public BTreeIndex {

public:  // types

    typedef eckit::FixedString<KEYSIZE> BTreeKey;
    typedef eckit::BTree<BTreeKey, PAYLOAD, RECSIZE> BTreeStore;

public:  // methods

    TBTreeIndex(const eckit::PathName& path, bool readOnly, off_t offset);
    ~TBTreeIndex();

private:  // methods

    virtual bool get(const std::string& key, FieldRef& data) const;
    virtual bool set(const std::string& key, const FieldRef& data);
    virtual void flush();
    virtual void sync();
    virtual void flock();
    virtual void funlock();
    virtual void visit(BTreeIndexVisitor& visitor) const;
    virtual void preload();

private:  // members

    mutable BTreeStore btree_;
};


template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::TBTreeIndex(const eckit::PathName& path, bool readOnly, off_t offset) :
    btree_(path, readOnly, offset) {}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::~TBTreeIndex() {
    btree_.funlock();
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
bool TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::get(const std::string& key, FieldRef& data) const {
    BTreeKey k(key);
    PAYLOAD payload;
    bool found = btree_.get(k, payload);
    if (found) {
        data = FieldRef(payload);
    }
    return found;
}


template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
bool TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::set(const std::string& key, const FieldRef& data) {
    BTreeKey k(key);
    PAYLOAD payload(data);
    return btree_.set(k, payload);
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::flush() {
    btree_.flush();
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::sync() {
    btree_.sync();
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::flock() {
    btree_.flock();
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::funlock() {
    btree_.funlock();
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
class TBTreeIndexVisitor {
    BTreeIndexVisitor& visitor_;

public:

    TBTreeIndexVisitor(BTreeIndexVisitor& visitor) : visitor_(visitor) {}

    // BTree::range() expect a STL like collection

    void clear() {}

    void push_back(const typename TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::BTreeStore::result_type& kv) {
        visitor_.visit(kv.first, kv.second);
    }
};

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::visit(BTreeIndexVisitor& visitor) const {
    TBTreeIndexVisitor<KEYSIZE, RECSIZE, PAYLOAD> v(visitor);
    btree_.range("", "\255", v);
}

template <int KEYSIZE, int RECSIZE, typename PAYLOAD>
void TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>::preload() {
    btree_.preload();
}


//----------------------------------------------------------------------------------------------------------------------


#define BTREE(KEYSIZE, RECSIZE, PAYLOAD)                                                                         \
    struct BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD : public TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD> {        \
        BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD(const eckit::PathName& path, bool readOnly, off_t offset) : \
            TBTreeIndex<KEYSIZE, RECSIZE, PAYLOAD>(path, readOnly, offset){};                                    \
    };                                                                                                           \
    static BTreeIndexBuilder<BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD>                                       \
        maker_BTreeIndex_##KEYSIZE##_##RECSIZE##_##PAYLOAD("BTreeIndex_" #KEYSIZE "_" #RECSIZE "_" #PAYLOAD)

BTREE(32, 65536, FieldRefReduced);
BTREE(32, 65536, FieldRefFull);
BTREE(32, 4194304, FieldRefReduced);
BTREE(64, 65536, FieldRefReduced);


//----------------------------------------------------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {}

static std::string defaultIndexType = "BTreeIndex";
static std::string wideIndexType = "BTreeIndex64";

const std::string& BTreeIndex::defaultType(size_t keySize) {
    static std::string fdbIndexType = eckit::Resource<std::string>("fdbIndexType;$FDB_INDEX_TYPE", "");

    return fdbIndexType.empty() ? (keySize >= 8 ? wideIndexType : defaultIndexType) : fdbIndexType;
}

static BTreeIndexBuilder<BTreeIndex_32_65536_FieldRefReduced> defaultIndex(defaultIndexType);
static BTreeIndexBuilder<BTreeIndex_64_65536_FieldRefReduced> wideIndex(wideIndexType);  // 64 char-limit for key

static BTreeIndexBuilder<BTreeIndex_32_65536_FieldRefFull> PointDBIndex("PointDBIndex");
static BTreeIndexBuilder<BTreeIndex_32_4194304_FieldRefReduced> BTreeIndex4MB("BTreeIndex4MB");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
