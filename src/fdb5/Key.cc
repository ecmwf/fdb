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
#include "fdb5/Rule.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/KeywordHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


Key::Key(const Handlers* handlers) :
    keys_(),
    handlers_(handlers),
    rule_(0)
{
}

Key::Key(const std::string& s) :
    keys_(),
    handlers_(0)
{
    NOTIMP;
}

Key::Key(const StringDict& keys) :
    keys_(keys),
    handlers_(0)
{
}

void Key::handlers(const Handlers* handlers) {
    handlers_ = handlers;
}

const Handlers* Key::handlers() const {
    return handlers_;
}

void Key::rule(const Rule* rule) {
    rule_ = rule;
}

const Rule* Key::rule() const {
    return rule_;
}

void Key::clear()
{
    keys_.clear();
    names_.clear();
}

void Key::set(const std::string& k, const std::string& v) {
    keys_[k] = v;
}

void Key::unset(const std::string& k) {
    keys_.erase(k);
    ASSERT(names_.back() == k);
}

void Key::push(const std::string& k, const std::string& v) {
    keys_[k] = v;
    names_.push_back(k);
}

void Key::pop(const std::string& k) {
    keys_.erase(k);
    ASSERT(names_.back() == k);
    names_.pop_back();
}

const std::string& Key::get( const std::string& k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if( i == keys_.end() ) {
        std::ostringstream oss;
        oss << "Key::get() failed for [" + k + "] in " << *this;
        throw SeriousBug(oss.str(), Here());
    }

    return i->second;
}

std::string Key::valuesToString() const
{
    ASSERT(names_.size() == keys_.size());

    std::ostringstream oss;

    const char *sep = "";

    for(StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());

        oss << sep;
        if(handlers_)
        {
             if(!(*i).second.empty()) {
                 handlers_->lookupHandler(*j).toKey(oss, *j, (*i).second);
             }
        }
        else {
            oss << (*i).second;
        }
        sep = ":";
    }

    return oss.str();
}

void Key::load(std::istream& s)
{
    NOTIMP;
    // std::string params;
    // s >> params;

    // Tokenizer parse(sep);
    // std::vector<std::string> result;
    // parse(params,result);

    // ASSERT( result.size() % 2 == 0 ); // even number of entries

    // clear();
    // for( size_t i = 0; i < result.size(); ++i,++i ) {
    //     set(result[i], result[i+1]);
    // }
}

void Key::dump(std::ostream& s) const
{
    NOTIMP;
    // s << sep;
    // for(StringDict::const_iterator ktr = keys_.begin(); ktr != keys_.end(); ++ktr) {
    //     s << ktr->first << sep << ktr->second << sep;
    // }
}

void Key::print(std::ostream &out) const
{
    if(names_.size() == keys_.size()) {
        const char *sep = "";
        out << "{";
        for(StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
            StringDict::const_iterator i = keys_.find(*j);
            ASSERT(i != keys_.end());
            out << sep << *j << '=' << i->second;
            sep = ",";

        }
        out << "}";

        if(rule_) {
            out << " (" << *rule_ << ")";
        }
    }
    else {
        out << keys_;
        if(rule_) {
            out << " (" << *rule_ << ")";
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
