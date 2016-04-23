/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/MatchOptional.h"
#include "fdb5/Key.h"
#include "eckit/types/Types.h"
#include "fdb5/TypesRegistry.h"

namespace fdb5 {

static std::string empty;

//----------------------------------------------------------------------------------------------------------------------

MatchOptional::MatchOptional(const std::string& def) :
    Matcher(),
    default_(def)
{
}

MatchOptional::~MatchOptional()
{
}

bool MatchOptional::match(const std::string& keyword, const Key& key) const
{
    return true;
}

bool MatchOptional::optional() const {
    return true;
}


const std::string& MatchOptional::value(const Key& key, const std::string& keyword) const {
    eckit::StringDict::const_iterator i = key.dict().find(keyword);

    if(i == key.dict().end()) {
        return default_;
    }

    return key.get(keyword);
}

const std::string& MatchOptional::defaultValue() const {
    return default_;
}

void MatchOptional::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const
{
    registry.dump(s, keyword);
    s << '?' << default_;
}

void MatchOptional::print(std::ostream& out) const
{
    out << "MatchOptional()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
