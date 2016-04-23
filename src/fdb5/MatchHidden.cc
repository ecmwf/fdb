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

#include "fdb5/MatchHidden.h"
#include "fdb5/Key.h"
#include "eckit/types/Types.h"
#include "fdb5/TypesRegistry.h"

namespace fdb5 {

static std::string empty;

//----------------------------------------------------------------------------------------------------------------------

MatchHidden::MatchHidden(const std::string& def) :
    Matcher(),
    default_(def)
{
}

MatchHidden::~MatchHidden()
{
}

bool MatchHidden::match(const std::string& keyword, const Key& key) const
{
    return true;
}

bool MatchHidden::optional() const {
    return true;
}

const std::string& MatchHidden::value(const Key& key, const std::string& keyword) const {
    return default_;
}

const std::string& MatchHidden::defaultValue() const {
    return default_;
}

void MatchHidden::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const
{
    registry.dump(s, keyword);
    s << '-' << default_;
}

void MatchHidden::print(std::ostream& out) const
{
    out << "MatchHidden()";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
