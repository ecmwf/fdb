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
#include "eckit/parser/Tokenizer.h"

#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

const char *sep = "/";

Key::Key() :
    keys_(),
    usedKeys_(0)
{
}

Key::Key(const std::string& s) :
    keys_(),
    usedKeys_(0)
{
    NOTIMP;
}

Key::Key(const StringDict& keys) :
    keys_(keys),
    usedKeys_(0)
{
}

void Key::clear()
{
    keys_.clear();
    if(usedKeys_) {
        (*usedKeys_).clear();
    }
}

void Key::set(const std::string& k, const std::string& v) {
    keys_[k] = v;
    if(usedKeys_) {
        (*usedKeys_)[k] = false;
    }
}

void Key::unset(const std::string& k) {
    keys_.erase(k);
    if(usedKeys_) {
        (*usedKeys_).erase(k);
    }
}

const std::string& Key::get( const std::string& k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if( i == keys_.end() ) {
        std::ostringstream oss;
        oss << "Key::get() failed for [" + k + "] in " << *this;
        throw SeriousBug(oss.str(), Here());
    }

    if(usedKeys_) {
        (*usedKeys_)[k] = true;
    }

    return i->second;
}

Key Key::subkey(const std::vector<std::string>& pattern) const
{
    eckit::StringDict r;
    for(std::vector<std::string>::const_iterator i = pattern.begin(); i != pattern.end(); ++i) {
        r[*i] = get(*i);
    }
    return Key(r);
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

std::string Key::toIndexForm() const
{
    std::string result(sep);
    StringDict::const_iterator ktr = keys_.begin();
    for(; ktr != keys_.end(); ++ktr)
        result += ktr->first + sep + ktr->second + sep;
    return result;
}

std::string Key::valuesToString() const
{
    const char *sep = ":";
    std::string result(sep);
    StringDict::const_iterator ktr = keys_.begin();
    for(; ktr != keys_.end(); ++ktr)
        result += ktr->second + sep;
    return result;
}

void Key::checkUsedKeys() const
{
    if(usedKeys_) {
        std::ostringstream oss;
        bool ok = true;
        const char* sep = "Unused keys: ";
        for(std::map<std::string, bool>::const_iterator i = (*usedKeys_).begin(); i != (*usedKeys_).end(); ++i) {
            if(!i->second) {
                oss << sep << i->first; sep = ",";
                ok = false;
            }
        }
        if(!ok) {
            throw SeriousBug(oss.str(), Here());
        }
    }
}

void Key::load(std::istream& s)
{
    std::string params;
    s >> params;

    Tokenizer parse(sep);
    std::vector<std::string> result;
    parse(params,result);

    ASSERT( result.size() % 2 == 0 ); // even number of entries

    clear();
    for( size_t i = 0; i < result.size(); ++i,++i ) {
        set(result[i], result[i+1]);
    }
}

void Key::dump(std::ostream& s) const
{
    s << sep;
    for(StringDict::const_iterator ktr = keys_.begin(); ktr != keys_.end(); ++ktr) {
        s << ktr->first << sep << ktr->second << sep;
    }
}

void Key::setUsedKeys(std::map<std::string, bool>* usedKeys) const
{
    ASSERT(usedKeys);
    usedKeys_ = usedKeys;
    for(StringDict::const_iterator ktr = keys_.begin(); ktr != keys_.end(); ++ktr) {
        (*usedKeys_)[ktr->first] = false;
    }
}

void Key::print(std::ostream &out) const
{
    out << keys_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
