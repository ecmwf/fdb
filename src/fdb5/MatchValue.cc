/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Key.h"
#include "fdb5/MatchValue.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MatchValue::MatchValue(const std::string& value) :
    Matcher(),
    value_(value)
{
}

MatchValue::~MatchValue()
{
}

bool MatchValue::match(const std::string& keyword, const Key& key) const
{
    StringDict::const_iterator i = key.dict().find(keyword);

    if(i == key.dict().end()) {
        return false;
    }

    return ( i->second == value_ );
}

void MatchValue::dump(std::ostream& s, const std::string& keyword) const
{
    s << keyword << "=" << value_;
}

void MatchValue::print(std::ostream& out) const
{
    out << "MatchValue()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
