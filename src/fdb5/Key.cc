/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/types/Types.h"

#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Key::Key() : keys_()
{
}

Key::Key(const StringDict& keys) : keys_(keys)
{
}

void Key::clear()
{
    keys_.clear();
}

void Key::set(const std::string& k, const std::string& v) { 
    keys_[k] = v; 
}

const std::string& Key::get( const std::string& k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if( i == keys_.end() ) {
        throw SeriousBug("Key::get() failed for [" + k + "]");
    }
    return i->second;
}

Key Key::subkey(const std::vector<std::string>& pattern) const
{
    Key r;
    for(std::vector<std::string>::const_iterator i = pattern.begin(); i != pattern.end(); ++i) {
        r.set( *i, get(*i));
    }
    return r;
}

bool Key::match(const Key& partial) const
{
    const StringDict& p = partial.keys_;
    for(StringDict::const_iterator i = p.begin(); i != p.end(); ++i) {
        StringDict::const_iterator j = keys_.find(i->first);
        if( !( j != keys_.end() && j->second == i->second) ) {
            return false;
        }
    }
    return true;
}

void Key::print(std::ostream &out) const
{
    out << keys_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
