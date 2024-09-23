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
    explicit Reverse(const Key& key): key_ {key} { }

    Key::const_reverse_iterator begin() const { return key_.rbegin(); }

    Key::const_reverse_iterator end() const { return key_.rend(); }

private:
    const Key& key_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

Key::Key(std::shared_ptr<const TypesRegistry> reg, bool canonical): registry_(std::move(reg)), canonical_(canonical) { }

Key::Key(const std::string& s, const Rule* rule): registry_(rule ? rule->registry() : nullptr) {

    eckit::Tokenizer  parse(":", true);
    eckit::StringList values;
    parse(s, values);

    if (rule) { rule->fill(*this, values); }
}

Key::Key(const eckit::StringDict& keys, std::shared_ptr<const TypesRegistry> reg):
    keys_(keys), registry_(std::move(reg)) {

    eckit::StringDict::const_iterator it = keys.begin();
    eckit::StringDict::const_iterator end = keys.end();
    for (; it != end; ++it) {
        names_.emplace_back(it->first);
    }
}

Key::Key(eckit::Stream& s, std::shared_ptr<const TypesRegistry> reg):
    registry_(std::move(reg)), canonical_(registry_ == nullptr) {
    decode(s);
}

Key::Key(std::initializer_list<std::pair<const std::string, std::string>> l, std::shared_ptr<const TypesRegistry> reg):
    keys_(l), registry_(std::move(reg)), canonical_(registry_ == nullptr) {

    for (const auto& kv : keys_) {
        names_.emplace_back(kv.first);
    }
}

Key Key::parseStringUntyped(const std::string& s) {

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

Key Key::parseString(const std::string& s, std::shared_ptr<const TypesRegistry> registry) {

    eckit::Tokenizer parse1(",");
    eckit::Tokenizer parse2("=");

    Key ret(std::move(registry));

    eckit::StringList vals;
    parse1(s, vals);

    for (const auto& bit : vals) {
        eckit::StringList kv;
        parse2(bit, kv);
        ASSERT(kv.size() == 2);

        std::string v = ret.registry().lookupType(kv[0]).tidy(kv[0], kv[1]);

        if (ret.find(kv[0]) == ret.end()) {
            ret.push(kv[0], v);
        } else {
            ret.set(kv[0], v);
        }
    }

    return ret;
}

void Key::decode(eckit::Stream& s) {

    registry_ = nullptr;
    canonical_ = true;
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
    const TypesRegistry* registry = canonical_ ? nullptr : &this->registry();

    s << keys_.size();
    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        s << i->first << (registry ? canonicalise(i->first, i->second) : i->second);
    }

    s << names_.size();
    for (eckit::StringList::const_iterator i = names_.begin(); i != names_.end(); ++i) {
        s << (*i);
        s << (registry ? registry->lookupType(*i).type() : "");
    }
}

std::set<std::string> Key::keys() const {

    std::set<std::string> k;

    for (eckit::StringDict::const_iterator i = keys_.begin(); i != keys_.end(); ++i) {
        k.insert(i->first);
    }

    return k;
}

void Key::clear() {
    registry_ = nullptr;
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

void Key::pushFrom(const Key& other) {
    for (const auto& [keyword, value] : Reverse(other)) { push(keyword, value); }
}

void Key::popFrom(const Key& other) {
    for (const auto& [keyword, value] : other) { pop(keyword); }
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

    return values.find(canonicalise(key, i->second)) != values.end();
}

bool Key::match(const std::string &key, const eckit::DenseSet<std::string> &values) const {

    eckit::StringDict::const_iterator i = find(key);
    if (i == end()) {
        return false;
    }

    // by default we use the exact request value. In case of mismatch, we try to canonicalise it
    return values.find(i->second) != values.end() || values.find(canonicalise(key, i->second)) != values.end();
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

void Key::registry(std::shared_ptr<const TypesRegistry> reg) {
    registry_ = std::move(reg);
}

const TypesRegistry& Key::registry() const {
    if (!registry_) {
        std::stringstream ss;
        ss << "TypesRegistry has not been set for Key " << (*this) << " prior to use";
        throw eckit::SeriousBug(ss.str(), Here());
    }

    return *registry_;
}

const void* Key::reg() const {
    return registry_.get();
}

std::string Key::canonicalise(const std::string& keyword, const std::string& value) const {
    if (value.empty() || canonical_) {
        return value;
    } else {
        return this->registry().lookupType(keyword).toKey(keyword, value);
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
        oss << canonicalise(*j, i->second);

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

    const TypesRegistry* registry = canonical_ ? nullptr : &this->registry();

    for (Key::const_iterator j = begin(); j != end(); ++j) {
        const std::string& keyword = (*j).first;
        Key::const_iterator k = other.find(keyword);
        if (k == other.end()) {
            missing.insert(keyword);
        }
        else {
            if(checkAlsoValues && !canonical_ && !registry->lookupType(keyword).match(keyword, k->second, j->second) ) {
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

    const TypesRegistry* registry = canonical_ ? nullptr : &this->registry();

    for (eckit::StringList::const_iterator j = names_.begin(); j != names_.end(); ++j) {

        eckit::StringDict::const_iterator i = keys_.find(*j);

        ASSERT(i != keys_.end());
        ASSERT(!(*i).second.empty());

        res[*j] = canonical_ ? (*i).second : registry->lookupType(*j).tidy(*j, (*i).second);
    }

    return res;
}

void Key::print(std::ostream &out) const {
    if (names_.size() == keys_.size()) {
        out << "{" << toString() << "}";
    } else {
        out << keys_;
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
