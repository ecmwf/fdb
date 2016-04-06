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
   
    typedef eckit::FixedString<256> BTreeKey;
    
public: // methods
    
	BTreeIndex( const eckit::PathName& path, Index::Mode m );
    
    virtual ~BTreeIndex();
    
    virtual bool    exists( const Key& key ) const;

    virtual bool    get( const Key& key, Field& field ) const;
    
    virtual Field   get( const Key& key ) const;
    
    virtual void    put( const Key& key, const Field& field );

    virtual bool    remove( const Key& key );

	virtual void	flush();

    /// apply a non-const operation to all entries of the Index
    virtual void apply( Index::Op& op );
    /// apply a const operation to all entries of the Index
    virtual void apply( Index::ConstOp& op ) const;

    friend std::ostream& operator<<(std::ostream& s,const BTreeIndex& x) { x.print(s); return s; }
    
private: // types
    
    typedef FileStore::FieldRef FieldRef;
    typedef eckit::BTree< BTreeKey , FieldRef, 65536 > BTreeStore;
    
private: // members    
    
    BTreeStore  btree_;

};

} // namespace fdb5

#endif
