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
#include <memory>
#include <utility>

#include "eckit/container/DenseSet.h"
#include "eckit/utils/Tokenizer.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

class Reverse {
public:
    explicit Reverse(const BaseKey& key): key_ {key} { }

    BaseKey::const_reverse_iterator begin() const { return key_.rbegin(); }

    BaseKey::const_reverse_iterator end() const { return key_.rend(); }

private:
    const BaseKey& key_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

void BaseKey::decode(eckit::Stream& s) {

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

void BaseKey::encode(eckit::Stream& s) const {

    s << keys_.size();
    for (const auto& [key_name, key_value] : keys_) {
        s << key_name << canonicalise(key_name, key_value);
    }

    s << names_.size();
    for (eckit::StringList::const_iterator i = names_.begin(); i != names_.end(); ++i) {
        s << (*i) << type(*i);
    }
}

std::set<std::string> BaseKey::keys() const {

    std::set<std::string> k;

    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        k.insert(i->first);
    }

    return k;
}

void BaseKey::clear() {
    keys_.clear();
    names_.clear();
}

void BaseKey::set(const std::string &k, const std::string &v) {

    ASSERT(names_.size() == keys_.size());

    eckit::StringDict::iterator it = keys_.find(k);
    if (it == keys_.end()) {
        names_.push_back(k);
        keys_[k] = v;
    } else {
        it->second = v;
    }
}

void BaseKey::unset(const std::string &k) {
    keys_.erase(k);
}

void BaseKey::push(const std::string &k, const std::string &v) {
    keys_[k] = v;
    names_.push_back(k);
}

void BaseKey::pop(const std::string &k) {
    keys_.erase(k);
    ASSERT(names_.back() == k);
    names_.pop_back();
}

void BaseKey::pushFrom(const BaseKey& other) {
    for (const auto& [keyword, value] : Reverse(other)) { push(keyword, value); }
}

void BaseKey::popFrom(const BaseKey& other) {
    for (const auto& [keyword, value] : other) { pop(keyword); }
}

const std::string &BaseKey::get( const std::string &k ) const {
    eckit::StringDict::const_iterator i = keys_.find(k);
    if ( i == keys_.end() ) {
        std::ostringstream oss;
        oss << "BaseKey::get() failed for [" + k + "] in " << *this;
        throw eckit::SeriousBug(oss.str(), Here());
    }

    return i->second;
}

bool BaseKey::match(const BaseKey& other) const {

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


bool BaseKey::match(const metkit::mars::MarsRequest& request) const {

    for (const auto& k : request.params()) {

        const_iterator j = find(k);
        if (j == end()) {
            return false;
        }

        bool found=false;
        auto values = request.values(k);
        std::string can = canonicalise(k, j->second);
        for (auto it = values.cbegin(); !found && it != values.cend(); it++) {
            found = can == canonicalise(k, *it);
        }
        if (!found) {
            return false;
        }
    }

    return true;
}

bool BaseKey::match(const std::string &key, const eckit::DenseSet<std::string> &values) const {

    eckit::StringDict::const_iterator i = find(key);
    if (i == end()) {
        return false;
    }

    // by default we use the exact request value. In case of mismatch, we try to canonicalise it
    return values.find(i->second) != values.end() || values.find(canonicalise(key, i->second)) != values.end();
}

bool BaseKey::partialMatch(const metkit::mars::MarsRequest& request) const {

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

std::string BaseKey::canonicalValue(const std::string& keyword) const {

    eckit::StringDict::const_iterator it = keys_.find(keyword);
    ASSERT(it != keys_.end());

    return canonicalise(keyword, it->second);
}

std::string BaseKey::valuesToString() const {

    if(names_.size() != keys_.size()) {
        std::stringstream ss;
        ss << "names and keys size mismatch" << std::endl
        << "    names: " << names_.size() << "  " << names_ << std::endl
        << "    keys: " << keys_.size() << "  " << keys_ << std::endl;

        throw eckit::SeriousBug(ss.str());
    }

    std::ostringstream oss;
    const char *sep = "";

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {
        eckit::StringDict::const_iterator i = keys_.find(*j);
        ASSERT(i != keys_.end());

        oss << sep;
        oss << canonicalise(*j, i->second);

        sep = ":";
    }

    return oss.str();
}


const eckit::StringList& BaseKey::names() const {
    return names_;
}

std::string BaseKey::value(const std::string& key) const {

    eckit::StringDict::const_iterator it = keys_.find(key);
    ASSERT(it != keys_.end());
    return it->second;
}

const eckit::StringDict &BaseKey::keyDict() const {
    return keys_;
}

metkit::mars::MarsRequest BaseKey::request(const std::string& verb) const {
    metkit::mars::MarsRequest req(verb);

    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        req.setValue(i->first, i->second);
    }

    return req;
}


fdb5::BaseKey::operator std::string() const {
    ASSERT(names_.size() == keys_.size());
    return toString();
}

fdb5::BaseKey::operator eckit::StringDict() const
{
    eckit::StringDict res;

    ASSERT(names_.size() == keys_.size());

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {

        eckit::StringDict::const_iterator i = keys_.find(*j);

        ASSERT(i != keys_.end());
        ASSERT(!(*i).second.empty());

        res[*j] = canonicalise(*j, (*i).second);
    }

    return res;
}

void BaseKey::print(std::ostream &out) const {
    if (names_.size() == keys_.size()) {
        out << "{" << toString() << "}";
    } else {
        out << keys_;
    }
}

std::string BaseKey::toString() const {
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

Key::Key(const eckit::StringDict &keys) :
    BaseKey(keys) {}

Key::Key(eckit::Stream& s) {
    decode(s);
}

Key::Key(std::initializer_list<std::pair<const std::string, std::string>> l) :
    BaseKey(l) {}

Key Key::parseString(const std::string& s) {

    eckit::Tokenizer parse1(",");
    eckit::Tokenizer parse2("=");
    eckit::StringDict keys;

    eckit::StringList v;
    parse1(s, v);
    for (const auto& bit : v) {
        eckit::StringList kv;
        parse2(bit, kv);
        ASSERT(kv.size() == 2);
        keys.emplace(std::move(kv[0]), std::move(kv[1]));
    }

    return Key{keys};
}

std::string Key::canonicalise(const std::string& keyword, const std::string& value) const {
    return value;
}

std::string Key::type(const std::string& keyword) const {
    return "";
}

//----------------------------------------------------------------------------------------------------------------------

TypedKey::TypedKey(const Key& key, const TypesRegistry& reg) :
    BaseKey(key), registry_(std::cref(reg)) {}

TypedKey::TypedKey(const TypesRegistry& reg) :
    registry_(std::cref(reg)) {}

TypedKey::TypedKey(const std::string &s, const Rule& rule) :
    registry_(std::cref(rule.registry())) {

    eckit::Tokenizer parse(":", true);
    eckit::StringList values;
    parse(s, values);

    rule.fill(*this, values);
}

TypedKey::TypedKey(const eckit::StringDict &keys, const TypesRegistry& reg) :
    BaseKey(keys), registry_(std::cref(reg)) {
}

TypedKey::TypedKey(eckit::Stream& s, const TypesRegistry& reg) :
    registry_(std::cref(reg)) {
    decode(s);
}

TypedKey::TypedKey(std::initializer_list<std::pair<const std::string, std::string>> l, const TypesRegistry& reg) :
    BaseKey(l), registry_(std::cref(reg)) {}

TypedKey TypedKey::parseString(const std::string &s, const TypesRegistry& registry) {

    eckit::Tokenizer parse1(",");
    eckit::Tokenizer parse2("=");
    TypedKey ret(std::move(registry));

    eckit::StringList vals;
    parse1(s, vals);

    for (const auto& bit : vals) {
        eckit::StringList kv;
        parse2(bit, kv);
        ASSERT(kv.size() == 2);

        std::string v = ret.registry().lookupType(kv[0]).tidy(kv[1]);

        if (ret.find(kv[0]) == ret.end()) {
            ret.push(kv[0], v);
        } else {
            ret.set(kv[0], v);
        }
    }

    return ret;
}

void TypedKey::validateKeys(const BaseKey& other, bool checkAlsoValues) const {

    eckit::StringSet missing;
    eckit::StringSet mismatch;

    for (BaseKey::const_iterator j = other.begin(); j != other.end(); ++j) {
        const std::string& keyword = (*j).first;
        BaseKey::const_iterator k = find(keyword);
        if (k == keys_.end()) {
            missing.insert(keyword);
        }
        else {
            if(checkAlsoValues && !registry_.get().lookupType(keyword).match(keyword, j->second, k->second)) {
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

        throw eckit::SeriousBug(oss.str());
    }
}

void TypedKey::registry(const TypesRegistry& reg) {
    registry_ = std::cref(reg);
}

const TypesRegistry& TypedKey::registry() const {
    return registry_.get();
}

std::string TypedKey::canonicalise(const std::string& keyword, const std::string& value) const {
    if (value.empty()) {
        return value;
    } else {
        return registry().lookupType(keyword).toKey(value);
    }
}

std::string TypedKey::type(const std::string& keyword) const {
    return registry().lookupType(keyword).type();
}

Key TypedKey::canonical() const {
    Key key{};

    for (const auto& name: names_) {
        auto m = keys_.find(name);
        ASSERT(m != keys_.end());

        key.set(name, canonicalise(name, m->second));
    }

    return key;
}

eckit::Stream& operator>>(eckit::Stream& s, TypedKey& x) {
    static TypesRegistry emptyTypesRegistry{};

    x = TypedKey(s, emptyTypesRegistry);
    return s;
}

} // namespace fdb5
