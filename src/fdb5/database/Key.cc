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

#include "fdb5/database/Key.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/types/Type.h"
#include "fdb5/config/MasterConfig.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Key::Key() :
    keys_(),
    rule_(0) {
}

Key::Key(const std::string &s, const Rule *rule) :
    keys_(),
    rule_(0) {
    eckit::Tokenizer parse(":", true);
    eckit::StringList values;
    parse(s, values);

    ASSERT(rule);
    rule->fill(*this, values);
}

Key::Key(const std::string &s) :
    keys_(),
    rule_(0) {

    const TypesRegistry &registry = this->registry();

    eckit::Tokenizer parse1(",");
    eckit::StringList v;

    parse1(s, v);

    eckit::Tokenizer parse2("=");
    for (eckit::StringList::const_iterator i = v.begin(); i != v.end(); ++i) {
        eckit::StringList kv;
        parse2(*i, kv);
        ASSERT(kv.size() == 2);

        const Type &t = registry.lookupType(kv[0]);

        std::string v = t.tidy(kv[0], kv[1]);

        if (find(kv[0]) == end()) {
            push(kv[0], v);
        } else {
            set(kv[0], v);
        }
    }

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
    const TypesRegistry& registry = this->registry();

    s << keys_.size();
    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        const Type &t = registry.lookupType(i->first);
        std::ostringstream oss;
        t.toKey(oss, i->first, i->second);
        s << i->first << oss.str();

    }

    s << names_.size();
    for (eckit::StringList::const_iterator i = names_.begin(); i != names_.end(); ++i) {
        const Type &t = registry.lookupType(*i);
        s << (*i);
        s << t.type();
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


const std::string &Key::get( const std::string &k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if ( i == keys_.end() ) {
        std::ostringstream oss;
        oss << "Key::get() failed for [" + k + "] in " << *this;
        throw eckit::SeriousBug(oss.str(), Here());
    }

    return i->second;
}

bool Key::match(const Key& other) const {
    for (const_iterator i = other.begin(); i != other.end(); ++i) {
        const_iterator j = find(i->first);
        if (j == end()) {
            return false;
        }

        if (j->second != i->second) {
            return false;
        }

    }
    return true;
}


const TypesRegistry& Key::registry() const {
    return rule_ ? rule_->registry() : MasterConfig::instance().schema().registry();
}

std::string Key::valuesToString() const {
    ASSERT(names_.size() == keys_.size());

    std::ostringstream oss;
    const TypesRegistry &registry = this->registry();

    const char *sep = "";

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        eckit::StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());

        oss << sep;
        if (!(*i).second.empty()) {
            registry.lookupType(*j).toKey(oss, *j, (*i).second);
        }

        sep = ":";
    }

    return oss.str();
}

void Key::validateKeysOf(const Key& other, bool checkAlsoValues) const
{
    eckit::StringSet missing;
    eckit::StringSet mismatch;

    for (Key::const_iterator j = begin(); j != end(); ++j) {
        Key::const_iterator k = other.find((*j).first);
        if (k == other.end()) {
            missing.insert((*j).first);
        }
        else {
            if(checkAlsoValues && (k->second != j->second)) {
                mismatch.insert((*j).first + "=" + j->second + " and " + k->second);
            }
        }
    }

    if (missing.size() || mismatch.size()) {
        std::ostringstream oss;

        if(missing.size()) {
            oss << "Keys not used: " << missing << " ";
        }

        if(mismatch.size()) {
            oss << "Values mismatch: " << mismatch << " ";
        }

        if (rule()) {
            oss << *rule();
        }

        throw eckit::SeriousBug(oss.str());
    }
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
