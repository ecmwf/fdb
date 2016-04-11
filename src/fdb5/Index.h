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

#ifndef fdb5_Index_H
#define fdb5_Index_H

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

#include "fdb5/FileStore.h"
#include "fdb5/IndexAxis.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Index : private eckit::NonCopyable {

public: // types
    
    enum Mode { WRITE, READ };
        
    /// A field location on the FDB
    struct Field {

        Field() {}

        Field(const eckit::PathName& path, eckit::Offset offset, eckit::Length length ) :
            path_(path),
            offset_(offset),
            length_(length) {
        }

        eckit::PathName path_;
        eckit::Offset   offset_;
        eckit::Length   length_;

        /// @todo
        /// Should return here the information about bit encoding for direct value access
        /// this should also persits in some other class
        
        void load( std::istream& s );
        
        void dump( std::ostream& s ) const;
        
        friend std::ostream& operator<<(std::ostream& s,const Field& x) { x.print(s); return s; }
        
		void print( std::ostream& out ) const { out << path_ << "," << offset_ << "+" << length_ ; }
        
    };
    
    /// Non-const operation on the index entries
    struct Op : private eckit::NonCopyable {
        virtual void operator() ( Index& idx, const Key& key, const Field& field ) = 0;
    };
    
    /// Const operation on the index entries
    struct ConstOp : private eckit::NonCopyable {
        virtual void operator() ( const Index& idx, const Key& key, const Field& field ) = 0;
    };
    
public: // methods
    
	static Index* create(const std::string& type, const eckit::PathName& path, Index::Mode m);
    
	Index( const eckit::PathName& path, Index::Mode m );
    
    virtual ~Index();

    virtual bool    exists( const Key& key ) const = 0;

    virtual bool    get( const Key& key, Field& field ) const = 0;
    
    virtual Field   get( const Key& key ) const = 0;
    
    virtual void    put( const Key& key, const Field& field );

    virtual bool    remove( const Key& key ) = 0;
        
    virtual void flush() = 0;    

    /// apply a non-const operation to all entries of the Index
    virtual void apply( Index::Op& op ) = 0;
    /// apply a const operation to all entries of the Index
    virtual void apply( Index::ConstOp& op ) const = 0;

    virtual void print( std::ostream& out ) const;
    virtual void json( std::ostream& out ) const;
    
    friend std::ostream& operator<<(std::ostream& s, const Index& x) { x.print(s); return s; }
    
	eckit::PathName path() const { return path_; }

	Mode mode() const { return mode_; }

    const IndexAxis& axis() const { return axis_; }

protected: // methods

    virtual void put_( const Key& key, const Field& field ) = 0;

protected: // members

	Mode            mode_;

	eckit::PathName path_;

    FileStore   files_;
    IndexAxis   axis_;

};

//----------------------------------------------------------------------------------------------------------------------

struct PrintIndex : public Index::ConstOp 
{
    PrintIndex( std::ostream& out ) : out_(out) {}
    virtual void operator() ( const Index&, const Key& key, const Index::Field& field );
    std::ostream& out_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
