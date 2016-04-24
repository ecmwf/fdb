/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/BTreeIndex.h"
#include "fdb5/Index.h"
#include "fdb5/TextIndex.h"

namespace fdb5 {

//-----------------------------------------------------------------------------

Index* Index::create(const Key& key, const std::string& type, const eckit::PathName& path, Index::Mode m )
{
//    Log::info() << "creating Index @ [" << path << "] of type [" << type << "]" << std::endl;
//    Log::info() << BackTrace::dump() << std::endl;

    if( type == "BTreeIndex" )
        return new BTreeIndex(key, path, m);

    if( type == "TextIndex" )
        return new TextIndex(key, path, m);

    throw eckit::BadParameter( "Unrecognized fdb5::Index type " + type + " @ " + Here().asString() );
}

//-----------------------------------------------------------------------------

Index::Index(const Key& key, const eckit::PathName& path, Index::Mode m ) :
	mode_(m),
	path_(path),
    files_( path + ".files" ),
    axis_( path + ".axis" ),
    key_(key)
{
}

Index::~Index()
{
}

void Index::put(const Key& key, const Index::Field& field)
{
    axis_.insert(key);
    put_(key, field);
}


//-----------------------------------------------------------------------------

void Index::Field::load(std::istream& s)
{
    std::string spath;
    long long offset;
    long long length;
    s >> spath >> offset >> length;
    path_    = spath;
    offset_  = offset;
    length_  = length;
}

void Index::Field::dump(std::ostream& s) const
{
    s << path_ << " " << offset_ << " " << length_;
}

//-----------------------------------------------------------------------------

const Key& Index::key() const
{
    return key_;
}

//-----------------------------------------------------------------------------

} // namespace fdb5
