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
#include "eckit/io/FileHandle.h"
#include "fdb5/TextIndex.h"
#include "fdb5/Error.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TextIndex::TextIndex(const Key& key, const eckit::PathName& path, Index::Mode mode ) :
    Index(key, path, mode),
    flushed_(true),
    fdbCheckDoubleInsert_( eckit::Resource<bool>("fdbCheckDoubleInsert",false) )
{
    load( path );
}

TextIndex::~TextIndex()
{
    flush();
}

bool TextIndex::exists(const Key& key) const
{
    FieldStore::const_iterator itr = store_.find(key);
    return( itr != store_.end() );
}

bool TextIndex::get(const Key &key, Index::Field& field) const
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

TextIndex::Field TextIndex::get(const Key& key) const
{
    Field field;

    FieldStore::const_iterator itr = store_.find(key);
    if( itr == store_.end() ) {
        std::ostringstream oss;
        oss << "FDB key not found " << key;
        throw eckit::BadParameter( oss.str(), Here() );
    }

    const FieldRef& ref = itr->second;

	field.path_     = files_.get(ref.pathId_);
    field.offset_   = ref.offset_;
    field.length_   = ref.length_;

    return field;
}

void TextIndex::put_(const Key& key, const TextIndex::Field& field)
{
	ASSERT( mode() == Index::WRITE );

    if(fdbCheckDoubleInsert_ && store_.find(key) != store_.end()) {
        std::ostringstream oss;
        oss << "Duplicate FDB entry with key: " << key << " -- This may be a schema bug in the fdbRules";
        throw fdb5::Error(Here(), oss.str());
    }

    FieldRef ref;
	ref.pathId_ = files_.insert( field.path_ );
    ref.offset_ = field.offset_;
    ref.length_ = field.length_;

    store_[key] = ref;

    flushed_ = false;
}

bool TextIndex::remove(const Key& key)
{
    NOTIMP;

	ASSERT( mode() == Index::WRITE );

    FieldStore::iterator itr = store_.find(key);
    if( itr != store_.end() )
        store_.erase(itr);
    return ( itr != store_.end() );
}

void TextIndex::flush()
{
	ASSERT( mode() == Index::WRITE );

    Index::flush();
    if( !flushed_ )
		save( path_ );
}

void TextIndex::load(const eckit::PathName& path)
{
    std::ifstream in( path.asString().c_str() );

    std::string line;
    while( getline(in,line) )
    {
        if( line.size() )
        {
            std::istringstream is(line);

            Key k;
            k.load(is);

            FieldRef fref;
            fref.load(is);

            store_[k] = fref;
            // Log::info() << k << " ----> " << f << std::endl;
        }
    }

    if(in.bad()) {
        throw eckit::ReadError(path_.asString());
    }
}

void TextIndex::save(const eckit::PathName& path) const
{
    std::ostringstream os;

    for( FieldStore::const_iterator itr = store_.begin(); itr != store_.end(); ++itr )
    {
        itr->first.dump(os);
        os << " ";
        itr->second.dump(os);
        os << "\n";
    }

    eckit::FileHandle storage(path);
    storage.openForWrite(0);
    std::string data = os.str();
    storage.write( data.c_str(), data.size() );
    storage.close();
}

void TextIndex::print(std::ostream& out) const
{
    out << "TextIndex[]";
}

static IndexBuilder<TextIndex> builder("TextIndex");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
