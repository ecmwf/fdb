/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino

#ifndef fdb5_TextIndex_H
#define fdb5_TextIndex_H

#include "eckit/eckit.h"

#include "eckit/io/Length.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Offset.h"

#include "fdb5/Index.h"
#include "fdb5/FileStore.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TextIndex : public Index {

public: // methods
    
	TextIndex(const eckit::PathName& path, Index::Mode m);
    
    virtual ~TextIndex();

protected: // methods

    virtual bool    exists( const IndexKey& key ) const;

    virtual bool    get( const IndexKey& key, Field& field ) const;
    
    virtual Field   get( const IndexKey& key ) const;
    
    virtual void    put_( const IndexKey& key, const Field& field );

    virtual bool    remove( const IndexKey& key );
    
    virtual void flush();    

    /// apply a non-const operation to all entries of the Index
    virtual void apply( Index::Op& op );
    /// apply a const operation to all entries of the Index
    virtual void apply( Index::ConstOp& op ) const;
    
    void save(const eckit::PathName& path) const;
    void load(const eckit::PathName& path);    
    
private: // types
    
    typedef FileStore::FieldRef FieldRef;
    
    struct IdxCompare {
        bool operator()(const IndexKey& x, const IndexKey& y) const { return (x.str() > y.str()); }
    };
    
    typedef std::map< IndexKey, FieldRef, IdxCompare > FieldStore;
        
private: // members
    
    FieldStore  store_;

    bool flushed_;
    bool fdbCheckDoubleInsert_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
