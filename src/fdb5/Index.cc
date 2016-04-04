/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/eckit.h"

#include "eckit/os/BackTrace.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/FileHandle.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/BTreeIndex.h"
#include "fdb5/Index.h"
#include "fdb5/TextIndex.h"
#include "fdb5/Key.h"

using namespace eckit;


namespace fdb5 {

//-----------------------------------------------------------------------------

static const std::string sep("/");

//-----------------------------------------------------------------------------

Index::Key::Key() : built_(false)
{
}

Index::Key::Key(const fdb5::Key& k)
{
    this->operator=( k.dict() );
}

Index::Key::Key( const std::string& s )
{
    this->operator=(s);
}

Index::Key& Index::Key::operator=(const eckit::StringDict &p)
{
    built_ = false;
    params_.clear();
    params_ = p;
    rebuild();
    return *this;
}

Index::Key& Index::Key::operator=(const std::string &s)
{
    std::istringstream is(s);
    load(is);
    return *this;
}

void Index::Key::set(const std::string& key, const std::string& value)
{
    params_[key] = value;
    built_ = false;
}

void Index::Key::rebuild()
{
    if(!built_)
    {
        key_ = sep;
        StringDict::iterator ktr = params_.begin();
        for(; ktr != params_.end(); ++ktr)
            key_ += ktr->first + sep + ktr->second + sep;
        built_ = true;
    }
}

void Index::Key::load(std::istream& s)
{
    std::string params;
    s >> params;
    
    Tokenizer parse(sep);
    std::vector<std::string> result;
	parse(params,result);
    
    ASSERT( result.size() % 2 == 0 ); // even number of entries

    built_ = false;
    params_.clear();
    for( size_t i = 0; i < result.size(); ++i,++i )
        params_[ result[i] ] = result[i+1];
    
    rebuild();
}

void Index::Key::dump(std::ostream& s) const
{
    s << sep;
    for(StringDict::const_iterator ktr = params_.begin(); ktr != params_.end(); ++ktr)
        s << ktr->first << sep << ktr->second << sep;
}

//-----------------------------------------------------------------------------

Index* Index::create( const std::string& type, const PathName& path, Index::Mode m )
{
//    Log::info() << "creating Index @ [" << path << "] of type [" << type << "]" << std::endl;
//    Log::info() << BackTrace::dump() << std::endl;
    
    if( type == "BTreeIndex" )
		return new BTreeIndex(path,m);
    
    if( type == "TextIndex" )
		return new TextIndex(path,m);
    
    throw BadParameter( "Unrecognized fdb5::Index type " + type + " @ " + Here().asString() );
}

Index::Index(const PathName& path, Index::Mode m ) :
	mode_(m),
	path_(path),
	files_( path + ".files" )
{    
}

Index::~Index()
{
}

void Index::print( std::ostream& out ) const 
{
    fdb5::PrintIndex print(out);
    apply( print ); 
}

void Index::json(std::ostream &out) const
{
    NOTIMP;
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

void PrintIndex::operator ()(const Index &, const Index::Key &key, const Index::Field &field)
{
    out_ << "{" << key << ":" << field << "}\n";
}

//-----------------------------------------------------------------------------

} // namespace fdb5
