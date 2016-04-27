/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_BTreeIndex_H
#define fdb5_BTreeIndex_H

#include "eckit/eckit.h"

#include "eckit/container/BTree.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"

#include "fdb5/Index.h"

namespace fdb5 {

class BTreeIndex : public Index {

public: // types

    typedef eckit::FixedString<32> BTreeKey;

public: // methods

    BTreeIndex(const Key& key, const eckit::PathName& path, Index::Mode mode );

    virtual ~BTreeIndex();

private: // methods

    virtual bool    exists( const Key& key ) const;

    virtual bool    get( const Key& key, Field& field ) const;

    virtual Field   get( const Key& key ) const;

    virtual void    put_( const Key& key, const Field& field );

    virtual bool    remove( const Key& key );

    virtual void	flush();

    virtual void entries(EntryVisitor& visitor) const;

private: // methods

    virtual void list( std::ostream& out ) const;

    virtual void print( std::ostream& out ) const;

private: // types

    typedef FileStore::FieldRef FieldRef;
    typedef eckit::BTree< BTreeKey , FieldRef, 65536 > BTreeStore;
    friend class BTreeIndexVisitor;

private: // members

    BTreeStore  btree_;

    bool fdbCheckDoubleInsert_; ///< check for keys overwriting each other within same archive request (useful to check fdbRules are sane)

    bool dirty_;

};

} // namespace fdb5

#endif
