/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/parser/Tokenizer.h"

#include "fdb5/Key.h"
#include "fdb5/Rule.h"
#include "fdb5/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Key::Key() :
    keys_(),
    rule_(0) {
}

Key::Key(const std::string &s, const Rule* rule) :
    keys_(),
    rule_(0) {
    eckit::Tokenizer parse(":", true);
    eckit::StringList values;
    parse(s, values);

    ASSERT(rule);
    rule->fill(*this, values);
}

Key::Key(const eckit::StringDict &keys) :
    keys_(keys),
    rule_(0) {
}


Key::Key(eckit::Stream &s) :
    rule_(0) {
    size_t n;

    s >> n;
    std::string k;
    std::string v;
    for (size_t i = 0; i < n; ++i) {
        s >> k;
        s >> v;
        keys_[k] = v;
    }

    s >> n;
    for (size_t i = 0; i < n; ++i) {
        s >> k;
        s >> v; // this is the type (ignoring FTM)
        names_.push_back(k);
    }
}

void Key::encode(eckit::Stream &s) const {
    const TypesRegistry *registry = rule_ ? &rule_->registry() : 0;

    s << keys_.size();
    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        if (registry) {
            const Type &t = registry->lookupType(i->first);
            std::ostringstream oss;
            t.toKey(oss, i->first, i->second);
            s << i->first << oss.str();
        } else {
            s << i->first << i->second;
        }
    }

    s << names_.size();
    for (eckit::StringList::const_iterator i = names_.begin(); i != names_.end(); ++i) {
        if (registry) {
            const Type &t = registry->lookupType(*i);
            s << (*i);
            s << t.type();
        } else {
            s << (*i);
            s << "Default";
        }
    }
}

void Key::rule(const Rule *rule) {
    rule_ = rule;
}

const Rule *Key::rule() const {
    return rule_;
}

void Key::clear() {
    keys_.clear();
    names_.clear();
}

void Key::set(const std::string &k, const std::string &v) {
    keys_[k] = v;
}

void Key::unset(const std::string &k) {
    keys_.erase(k);
}

void Key::push(const std::string &k, const std::string &v) {
    keys_[k] = v;
    names_.push_back(k);
}

void Key::pop(const std::string &k) {
    keys_.erase(k);
    ASSERT(names_.back() == k);
    names_.pop_back();
}

bool Key::has(const std::string &k) const {
    return keys_.find(k) != keys_.end();
}

const std::string &Key::get( const std::string &k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if ( i == keys_.end() ) {
        std::ostringstream oss;
        oss << "Key::get() failed for [" + k + "] in " << *this;
        throw eckit::SeriousBug(oss.str(), Here());
    }

    return i->second;
}

const TypesRegistry *Key::registry() const {
    return rule_ ? &rule_->registry() : 0;
}

std::string Key::valuesToString() const {
    ASSERT(names_.size() == keys_.size());

    std::ostringstream oss;
    const TypesRegistry *registry = rule_ ? &rule_->registry() : 0;

    const char *sep = "";

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        eckit::StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());

        oss << sep;
        if (registry) {
            if (!(*i).second.empty()) {
                registry->lookupType(*j).toKey(oss, *j, (*i).second);
            }
        } else {
            oss << (*i).second;
        }
        sep = ":";
    }

    return oss.str();
}

void Key::load(std::istream &s) {
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

void Key::dump(std::ostream &s) const {
    NOTIMP;
    // s << sep;
    // for(StringDict::const_iterator ktr = keys_.begin(); ktr != keys_.end(); ++ktr) {
    //     s << ktr->first << sep << ktr->second << sep;
    // }
}

void Key::json(eckit::JSON &) const {

}

void Key::print(std::ostream &out) const {
    if (names_.size() == keys_.size()) {
        const char *sep = "";
        out << "{";
        for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
            eckit::StringDict::const_iterator i = keys_.find(*j);
            ASSERT(i != keys_.end());
            out << sep << *j << '=' << i->second;
            sep = ",";

        }
        out << "}";

        if (rule_) {
            out << " (" << *rule_ << ")";
        }
    } else {
        out << keys_;
        if (rule_) {
            out << " (" << *rule_ << ")";
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
