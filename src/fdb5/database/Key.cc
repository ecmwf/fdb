/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/container/DenseSet.h"
#include "eckit/utils/Tokenizer.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Key::Key() :
    keys_(),
    rule_(0) {}

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

    eckit::StringDict::const_iterator it = keys.begin();
    eckit::StringDict::const_iterator end = keys.end();
    for (; it != end; ++it) {
        names_.emplace_back(it->first);
    }
}

Key::Key(eckit::Stream& s) :
    rule_(nullptr) {
    decode(s);
}

void Key::decode(eckit::Stream& s) {

    ASSERT(rule_ == nullptr);

    keys_.clear();
    names_.clear();


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

void Key::encode(eckit::Stream& s) const {
    const TypesRegistry& registry = this->registry();

    s << keys_.size();
    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        const Type &t = registry.lookupType(i->first);
        s << i->first << canonicalise(i->first, i->second);

    }

    s << names_.size();
    for (eckit::StringList::const_iterator i = names_.begin(); i != names_.end(); ++i) {
        const Type &t = registry.lookupType(*i);
        s << (*i);
        s << t.type();
    }
}

std::set<std::string> Key::keys() const {

    std::set<std::string> k;

    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        k.insert(i->first);
    }

    return k;
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

    eckit::StringDict::iterator it = keys_.find(k);
    if (it == keys_.end()) {
        names_.push_back(k);
        keys_[k] = v;
    } else {
        it->second = v;
    }

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

        if (j->second != i->second && !i->second.empty()) {
            return false;
        }

    }
    return true;
}


bool Key::match(const metkit::mars::MarsRequest& request) const {

    std::vector<std::string> p = request.params();
    std::vector<std::string>::const_iterator k = p.begin();
    std::vector<std::string>::const_iterator kend = p.end();
    for (; k != kend; ++k) {

        const_iterator j = find(*k);
        if (j == end()) {
            return false;
        }

        const auto& values = request.values(*k);
        if (std::find(values.begin(), values.end(), j->second) == values.end()) {
            return false;
        }
    }

    return true;
}

bool Key::match(const Key& other, const eckit::StringList& ignore) const {

    for (const_iterator i = other.begin(); i != other.end(); ++i) {
        if (std::find(ignore.begin(), ignore.end(), i->first) != ignore.end())
            continue;

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

bool Key::match(const std::string &key, const std::set<std::string> &values) const {

    eckit::StringDict::const_iterator i = find(key);
    if (i == end()) {
        return false;
    }

    if (i->second.empty()) {
        return values.find(i->second) != values.end();
    }

    return values.find(canonicalise(key, i->second)) != values.end();
}

bool Key::match(const std::string &key, const eckit::DenseSet<std::string> &values) const {

    eckit::StringDict::const_iterator i = find(key);
    if (i == end()) {
        return false;
    }

    if (i->second.empty()) {
        return values.find(i->second) != values.end();
    }

    return values.find(canonicalise(key, i->second)) != values.end();
}

bool Key::partialMatch(const metkit::mars::MarsRequest& request) const {

    for (const auto& kv : *this) {

        const auto& values = request.values(kv.first, /* emptyOk */ true);

        if (!values.empty()) {
            if (std::find(values.begin(), values.end(), kv.second) == values.end()) {
                return false;
            }
        }
    }

    return true;
}


const TypesRegistry& Key::registry() const {
    if(rule_) {
        return rule_->registry();
    }
    else {
        Config config = LibFdb5::instance().defaultConfig();
        return config.schema().registry();
    }
}

std::string Key::canonicalise(const std::string& keyword, const std::string& value) const {
    if (value.empty()) {
        return value;
    } else {
        std::ostringstream oss;
        this->registry().lookupType(keyword).toKey(oss, keyword, value);
        return oss.str();
    }
}

std::string Key::canonicalValue(const std::string& keyword) const {

    eckit::StringDict::const_iterator it = keys_.find(keyword);
    ASSERT(it != keys_.end());

    return canonicalise(keyword, it->second);
}

std::string Key::valuesToString() const {

    ASSERT(names_.size() == keys_.size());

    std::ostringstream oss;
    const char *sep = "";

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        eckit::StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());

        oss << sep;
        if (!i->second.empty()) {
            oss << canonicalise(*j, i->second);
        }

        sep = ":";
    }

    return oss.str();
}


const eckit::StringList& Key::names() const {
    return names_;
}

std::string Key::value(const std::string& key) const {

    eckit::StringDict::const_iterator it = keys_.find(key);
    ASSERT(it != keys_.end());
    return it->second;
}

void Key::validateKeysOf(const Key& other, bool checkAlsoValues) const
{
    eckit::StringSet missing;
    eckit::StringSet mismatch;

    const TypesRegistry& registry = this->registry();

    for (Key::const_iterator j = begin(); j != end(); ++j) {
        const std::string& keyword = (*j).first;
        Key::const_iterator k = other.find(keyword);
        if (k == other.end()) {
            missing.insert(keyword);
        }
        else {
            if(checkAlsoValues && !registry.lookupType(keyword).match(keyword, k->second, j->second) ) {
                mismatch.insert((*j).first + "=" + j->second + " and " + k->second);
            }
        }
    }

    if (missing.size() || mismatch.size()) {
        std::ostringstream oss;

        if(missing.size()) {
            oss << "Keywords not used: " << missing << " ";
        }

        if(mismatch.size()) {
            oss << "Values mismatch: " << mismatch << " ";
        }

        oss << "for key " << *this << " validating " << other;

        if (rule()) {
            oss << " " << *rule();
        }

        throw eckit::SeriousBug(oss.str());
    }
}

const eckit::StringDict &Key::keyDict() const {
    return keys_;
}

metkit::mars::MarsRequest Key::request(std::string verb) const {
    metkit::mars::MarsRequest req(verb);

    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        req.setValue(i->first, i->second);
    }

    return req;
}


fdb5::Key::operator std::string() const {
    ASSERT(names_.size() == keys_.size());
    return toString();
}

fdb5::Key::operator eckit::StringDict() const
{
    eckit::StringDict res;

    ASSERT(names_.size() == keys_.size());

    const TypesRegistry &registry = this->registry();

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {

        eckit::StringDict::const_iterator i = keys_.find(*j);

        ASSERT(i != keys_.end());
        ASSERT(!(*i).second.empty());

        res[*j] = registry.lookupType(*j).tidy(*j, (*i).second);
    }

    return res;
}

void Key::print(std::ostream &out) const {
    if (names_.size() == keys_.size()) {
        out << "{" << toString() << "}";
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

std::string Key::toString() const {
    std::string res;
    const char *sep = "";
    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        eckit::StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());
        if (!i->second.empty()) {
            res += sep + *j + '=' + i->second;
            sep = ",";
        }
    }
    return res;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
