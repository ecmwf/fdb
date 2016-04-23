/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/FileHandle.h"

#include "fdb5/FileStore.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FileStore::FieldRef::load(std::istream &s)
{
    long long offset;
    long long length;
    s >> pathId_ >> offset >> length;
    offset_  = offset;
    length_  = length;
}

void FileStore::FieldRef::dump(std::ostream &s) const
{
    s << pathId_ << " " << offset_ << " " << length_;
}

//----------------------------------------------------------------------------------------------------------------------

FileStore::FileStore( const eckit::PathName& path ) :
    next_(0),
    path_(path),
    flushed_(true)
{
    if( path.exists() )
    {
        std::ifstream in;
        in.open( path_.asString().c_str() );

        std::string line;
        while( getline(in,line) )
        {
            if( line.size() )
            {
                std::istringstream is(line);
                FileStore::PathID id;
                std::string p;
                is >> id >> p;
                eckit::PathName path(p);
                paths_[id] = path;
                ids_[path] = id;
                next_ = std::max(id+1,next_);
                // Log::info() << id << " ----> " << p << std::endl;
            }
        }
        in.close();
    }
}

FileStore::~FileStore()
{
    flush();
}

FileStore::PathID FileStore::insert( const eckit::PathName& path )
{
	IdStore::iterator itr = ids_.find(path);
	if( itr != ids_.end() )
		return itr->second;

	FileStore::PathID current = next_;
	next_++;
	ids_[path] = current;
	paths_[current] = path;
	flushed_ = false;

	return current;
}

FileStore::PathID FileStore::get( const eckit::PathName& path ) const
{
	IdStore::const_iterator itr = ids_.find(path);
	ASSERT( itr != ids_.end() );
	return itr->second;
}

eckit::PathName FileStore::get(const FileStore::PathID id) const
{
    PathStore::const_iterator itr = paths_.find(id);
    ASSERT( itr != paths_.end() );
    return itr->second;
}

bool FileStore::exists(const PathID id ) const
{
    PathStore::const_iterator itr = paths_.find(id);
    return( itr != paths_.end() );
}

bool FileStore::exists(const eckit::PathName& path) const
{
    IdStore::const_iterator itr = ids_.find(path);
    return( itr != ids_.end() );
}

void FileStore::flush()
{
    if ( ! flushed_ )
    {
        std::ostringstream os;

        for( PathStore::const_iterator itr = paths_.begin(); itr != paths_.end(); ++itr )
            os << itr->first << " " << itr->second << "\n";

        eckit::FileHandle storage(path_);
        storage.openForWrite(0);
        std::string data = os.str();
        storage.write( data.c_str(), data.size() );
        storage.close();

        flushed_ = true;
    }
}

void FileStore::print( std::ostream& out ) const
{
    for( PathStore::const_iterator itr = paths_.begin(); itr != paths_.end(); ++itr )
		out << itr->first << " " << itr->second << "\n";
}

//-----------------------------------------------------------------------------

} // namespace fdb5
