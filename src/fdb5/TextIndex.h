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

class TextIndex : public Index {

public: // methods
    
	TextIndex(const eckit::PathName& path, Index::Mode m);
    
    virtual ~TextIndex();

    virtual bool    exists( const Key& key ) const;

    virtual bool    get( const Key& key, Field& field ) const;
    
    virtual Field   get( const Key& key ) const;
    
    virtual void    put( const Key& key, const Field& field );

    virtual bool    remove( const Key& key );
    
    virtual void flush();    

    /// apply a non-const operation to all entries of the Index
    virtual void apply( Index::Op& op );
    /// apply a const operation to all entries of the Index
    virtual void apply( Index::ConstOp& op ) const;

    friend std::ostream& operator<<(std::ostream& s,const TextIndex& x) { x.print(s); return s; }
    
protected: // methods

    void save(const eckit::PathName& path) const;
    void load(const eckit::PathName& path);    
    
private: // types
    
    typedef FileStore::FieldRef FieldRef;
    
    struct IdxCompare {
        bool operator()(const Key& x, const Key& y) const { return (x.str() > y.str()); }
    };
    
    typedef std::map< Key, FieldRef, IdxCompare > FieldStore;
        
private: // members
    
    bool        flushed_;
    
    FieldStore  store_;

};

} // namespace fdb5

#endif
