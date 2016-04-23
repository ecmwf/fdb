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
#include "fdb5/MatchAny.h"
#include "fdb5/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MatchAny::MatchAny(const std::set<std::string>& values) :
    Matcher(),
    values_(values)
{
}

MatchAny::~MatchAny()
{
}

bool MatchAny::match(const std::string& keyword, const Key& key) const
{
    eckit::StringDict::const_iterator i = key.dict().find(keyword);

    if(i == key.dict().end()) {
        return false;
    }

    return (values_.find(i->second) != values_.end());
}

void MatchAny::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const
{
    const char* sep = "";
    registry.dump(s, keyword);
    s << "=";
    for( std::set<std::string>::const_iterator i = values_.begin(); i != values_.end(); ++i) {
        s << sep << *i;
        sep = "/";
    }
}

void MatchAny::print(std::ostream& out) const
{
    out << "MatchAny()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
