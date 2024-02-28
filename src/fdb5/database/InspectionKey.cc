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
#include "fdb5/database/InspectionKey.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

InspectionKey::InspectionKey() :
    Key(), rule_(nullptr) {}

InspectionKey::InspectionKey(const Key& other) {
    for (const auto& name : other.names_) {
        names_.push_back(name);
    }
    for (const auto& k : other.keys_) {
        keys_[k.first] = k.second;
    }
}

InspectionKey::InspectionKey(const std::string &s, const Rule *rule) :
    Key(), rule_(nullptr) {
    eckit::Tokenizer parse(":", true);
    eckit::StringList values;
    parse(s, values);

    ASSERT(rule);
    rule->fill(*this, values);
}

InspectionKey::InspectionKey(const std::string &s) :
    Key(),
    rule_(nullptr) {

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

InspectionKey::InspectionKey(const eckit::StringDict &keys) :
    Key(keys), rule_(nullptr) {

    eckit::StringDict::const_iterator it = keys.begin();
    eckit::StringDict::const_iterator end = keys.end();
    for (; it != end; ++it) {
        names_.emplace_back(it->first);
    }
}

InspectionKey::InspectionKey(eckit::Stream& s) :
    rule_(nullptr) {
    decode(s);
}

Key InspectionKey::canonical() const {
    Key key;

    for (const auto& name: names_) {
        auto m = keys_.find(name);
        ASSERT(m != keys_.end());

        key.set(name, canonicalise(name, m->second));
    }

    return key;
}

void InspectionKey::rule(const Rule *rule) {
    rule_ = rule;
}

const TypesRegistry& InspectionKey::registry() const {
    if(rule_) {
        return rule_->registry();
    }
    else {
        return LibFdb5::instance().defaultConfig().schema().registry();
    }
}

std::string InspectionKey::canonicalise(const std::string& keyword, const std::string& value) const {
    if (value.empty()) {
        return value;
    } else {
        return this->registry().lookupType(keyword).toKey(keyword, value);
    }
}

fdb5::InspectionKey::operator eckit::StringDict() const
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

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
