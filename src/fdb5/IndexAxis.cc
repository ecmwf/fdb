/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/FileHandle.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/parser/JSON.h"

#include "fdb5/IndexAxis.h"
#include "fdb5/Index.h"
#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexAxis::IndexAxis(const eckit::PathName& path) :
    path_(path),
    readOnly_(false)
{
    if( path.exists() )
    {
        std::ifstream f;
        f.open( path_.asString().c_str() );
        JSONParser parser(f);

        eckit::Value v = parser.parse();
        JSONParser::toDictStrSet(v, axis_);

        f.close();

        readOnly_ = true;
    }

    Log::info() << *this << std::endl;
}

IndexAxis::~IndexAxis()
{
    Log::info() << *this << std::endl;
    if(!readOnly_) {

        FileHandle f(path_);

        f.openForWrite(0); AutoClose closer(f);

        std::ostringstream os;

        JSON j(os);
        json(j);

        Log::info() << "Axis JSON " << os.str() << std::endl;

        f.write(os.str().c_str(), os.str().size());
    }
}

void IndexAxis::insert(const Key& key)
{
    ASSERT(!readOnly_);

//    Log::info() << *this << std::endl;

    const StringDict& keymap = key.dict();

    for(StringDict::const_iterator i = keymap.begin(); i  != keymap.end(); ++i) {
        const std::string& keyword = i->first;
        const std::string& value   = i->second;
        axis_[keyword].insert(value);
    }
}

const StringSet& IndexAxis::values(const std::string& keyword) const
{
    AxisMap::const_iterator i = axis_.find(keyword);
    if(i == axis_.end()) {
        throw SeriousBug("Cannot find Axis: " + keyword);
    }
    return i->second;
}

void IndexAxis::print(std::ostream& out) const
{
    out << "IndexAxis("
        << "path=" << path_
        <<  ",axis=";
    __print_container(out, axis_);
    out  << ")";
}

void IndexAxis::json(JSON& j) const
{
    j << axis_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
