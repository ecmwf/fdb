/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/AIOHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/parser/Tokenizer.h"

#include "fdb5/TextIndex.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TextIndex::TextIndex( const PathName& path, Index::Mode m ) :
	Index(path,m),
	flushed_(true)
{
    load( path );
}

TextIndex::~TextIndex()
{
    flush();
}

bool TextIndex::exists(const IndexKey& key) const
{
    FieldStore::const_iterator itr = store_.find(key);
    return( itr != store_.end() );
}

bool TextIndex::get(const IndexKey &key, Index::Field& field) const
{
    FieldStore::const_iterator itr = store_.find(key);
    if( itr == store_.end() ) 
        return false;

    const FieldRef& ref = itr->second;
    
	field.path_     = files_.get(ref.pathId_);
    field.offset_   = ref.offset_; 
    field.length_   = ref.length_;

    return true;
}

TextIndex::Field TextIndex::get(const IndexKey& key) const
{
    Field field;
    
    FieldStore::const_iterator itr = store_.find(key);
    if( itr == store_.end() ) 
        throw BadParameter( std::string(" key with ") + key.str() + std::string(" not found @ ") + Here().asString() );

    const FieldRef& ref = itr->second;
    
	field.path_     = files_.get(ref.pathId_);
    field.offset_   = ref.offset_; 
    field.length_   = ref.length_;
    
    return field;
}

void TextIndex::put_(const IndexKey& key, const TextIndex::Field& field)
{
	ASSERT( mode() == Index::WRITE );

    FieldRef ref;
	ref.pathId_ = files_.insert( field.path_ );
    ref.offset_ = field.offset_; 
    ref.length_ = field.length_;
    
    store_[key] = ref;
    
    flushed_ = false;
}

bool TextIndex::remove(const IndexKey& key)
{
	ASSERT( mode() == Index::WRITE );

    FieldStore::iterator itr = store_.find(key);
    if( itr != store_.end() )
        store_.erase(itr);
    return ( itr != store_.end() );
}

void TextIndex::flush()
{
	ASSERT( mode() == Index::WRITE );

    files_.flush();
    if( !flushed_ )
		save( path_ );
}

void TextIndex::apply( Index::Op &op )
{
	ASSERT( mode() == Index::WRITE );

    Field field;
    for( FieldStore::iterator itr = store_.begin(); itr != store_.end(); ++itr )
    {
        FieldRef& ref = itr->second;
        
		field.path_     = files_.get(ref.pathId_);
        field.offset_   = ref.offset_; 
        field.length_   = ref.length_;
        
        op(*this,itr->first,field);
    }
}

void TextIndex::apply(ConstOp &op ) const
{
    Field field;
    for( FieldStore::const_iterator itr = store_.begin(); itr != store_.end(); ++itr )
    {
        const FieldRef& ref = itr->second;
        
		field.path_     = files_.get(ref.pathId_);
        field.offset_   = ref.offset_; 
        field.length_   = ref.length_;
        
        op(*this,itr->first,field);
    }
}

void TextIndex::load(const PathName& path)
{
    std::ifstream in;
    in.open( path.asString().c_str() );

    std::string line;
    while( getline(in,line) )
    {		
        if( line.size() )
        {
            std::istringstream is(line);

            IndexKey k;
            k.load(is);

            FieldRef fref;
            fref.load(is);

            store_[k] = fref;
            // Log::info() << k << " ----> " << f << std::endl;
        }
    }
    
    in.close();
}

void TextIndex::save(const PathName& path) const
{
    std::ostringstream os;
    
    for( FieldStore::const_iterator itr = store_.begin(); itr != store_.end(); ++itr )
    {
        itr->first.dump(os);
        os << " "; 
        itr->second.dump(os);
        os << "\n";
    }
              
    FileHandle storage(path);
    storage.openForWrite(0);
    std::string data = os.str();
    storage.write( data.c_str(), data.size() );
    storage.close();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
