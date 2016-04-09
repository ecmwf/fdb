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

#include "fdb5/BTreeIndex.h"
#include "fdb5/Error.h"

using namespace eckit;

namespace fdb5 {

//-----------------------------------------------------------------------------

BTreeIndex::BTreeIndex( const PathName& path, Index::Mode m ) :
	Index(path,m),
    btree_( path, bool( m == Index::READ ) ),
    fdbCheckDoubleInsert_( eckit::Resource<bool>("fdbCheckDoubleInsert", true) )
{
}

BTreeIndex::~BTreeIndex()
{
}

bool BTreeIndex::exists(const IndexKey &key) const
{
    BTreeKey k (key.str());
    FieldRef ignore;
    return const_cast<BTreeIndex*>(this)->btree_.get(k,ignore);
}

bool BTreeIndex::get(const IndexKey &key, Index::Field& field) const
{
    FieldRef ref;
    BTreeKey k (key.str());
    bool found = const_cast<BTreeIndex*>(this)->btree_.get(k,ref);
    if( found )
    {
		field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_; 
        field.length_   = ref.length_;
    }
    return found;
}

BTreeIndex::Field BTreeIndex::get(const IndexKey& key) const
{
    Field result;
    FieldRef ref;
    BTreeKey k (key.str());
    bool found = const_cast<BTreeIndex*>(this)->btree_.get(k,ref);
    if( !found )
        throw BadParameter( std::string(" key with ") + key.str() + std::string(" not found @ ") + Here().asString() );
    
	result.path_     = files_.get( ref.pathId_ );
    result.offset_   = ref.offset_; 
    result.length_   = ref.length_;
    
    return result;
}

void BTreeIndex::put_(const IndexKey& key, const BTreeIndex::Field& field)
{
    ASSERT( mode() == Index::WRITE );

    Log::info() << "BTreeIndex insert " << key << " = " << field << std::endl;

	BTreeKey k( key.str() );
    FieldRef ref;

	ref.pathId_ = files_.insert( field.path_ ); // inserts not yet in filestore
    ref.offset_ = field.offset_; 
    ref.length_ = field.length_;

    bool replace = btree_.set(k,ref);  // returns true if replace, false if new insert

    if(fdbCheckDoubleInsert_ && replace) {
        std::ostringstream oss;
        oss << "Duplicate FDB entry with key: " << key << " -- This may be a schema bug in the fdbRules";
        throw fdb5::Error(Here(), oss.str());
    }
}

bool BTreeIndex::remove(const IndexKey& key)
{
    ASSERT( mode() == Index::WRITE );

	BTreeKey k( key.str() );
	return btree_.remove(k);
}

void BTreeIndex::flush()
{
    ASSERT( mode() == Index::WRITE );

    files_.flush();
    btree_.flush();
}

void BTreeIndex::apply( Index::Op& op )
{
    ASSERT( mode() == Index::WRITE );

    IndexKey key;
    Field field;
    
    BTreeKey first("");
    BTreeKey last("~");
    std::vector< std::pair<BTreeKey,FieldRef> > all;
    btree_.range(first,last,all);
    for( size_t i = 0; i < all.size(); ++i )
    {
        key = all[i].first.asString();
        FieldRef& ref = all[i].second;
		field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_;
        field.length_   = ref.length_;
        op(*this,key,field);
    }
}

void BTreeIndex::apply( Index::ConstOp& op ) const
{    
    IndexKey key;
    Field field;
    
    BTreeKey first("");
    BTreeKey last("~");
    std::vector< std::pair<BTreeKey,FieldRef> > all;
    const_cast<BTreeIndex*>(this)->btree_.range(first,last,all);
    for( size_t i = 0; i < all.size(); ++i )
    {
        key = all[i].first.asString();
        FieldRef& ref = all[i].second;
		field.path_     = files_.get( ref.pathId_ );
        field.offset_   = ref.offset_;
        field.length_   = ref.length_;
        op(*this,key,field);
    }
}

//-----------------------------------------------------------------------------

} // namespace fdb5
