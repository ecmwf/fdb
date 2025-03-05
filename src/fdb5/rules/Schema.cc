/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstddef>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec Schema::classSpec_ = {
    &eckit::Streamable::classSpec(),
    "Schema",
};

eckit::Reanimator<Schema> Schema::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema() = default;

Schema::Schema(const eckit::PathName& path) {
    load(path);
}

Schema::Schema(std::istream& stream) {
    load(stream);
}

Schema::Schema(eckit::Stream& stream) : registry_{stream} {

    size_t numRules = 0;
    stream >> path_;
    stream >> numRules;
    rules_.reserve(numRules);
    for (size_t i = 0; i < numRules; i++) {
        rules_.emplace_back(new RuleDatabase(stream));
    }

    check();
}

void Schema::encode(eckit::Stream& stream) const {
    registry_.encode(stream);
    // stream << registry_;
    stream << path_;
    stream << rules_.size();
    for (const auto& rule : rules_) {
        rule->encode(stream);
    }
}

Schema::~Schema() {
    clear();
}

//----------------------------------------------------------------------------------------------------------------------

const RuleDatum& Schema::matchingRule(const Key& dbKey, const Key& idxKey) const {

    for (const auto& dbRule : rules_) {
        if (!dbRule->match(dbKey)) {
            continue;
        }
        for (const auto& idxRule : dbRule->rules()) {
            if (!idxRule->match(idxKey)) {
                continue;
            }
            /// @note returning first datum. could there be multiple datum per index ?
            for (const auto& datumRule : idxRule->rules()) {
                return *datumRule;
            }
        }
    }

    std::ostringstream msg;
    msg << "No rule is matching dbKey=" << dbKey << " and idxKey=" << idxKey;
    throw eckit::SeriousBug(msg.str(), Here());
}

const RuleDatabase& Schema::matchingRule(const Key& dbKey) const {

    for (const auto& rule : rules_) {
        if (rule->match(dbKey)) {
            return *rule;
        }
    }

    std::ostringstream msg;
    msg << "No rule is matching dbKey=" << dbKey;
    throw eckit::SeriousBug(msg.str(), Here());
}

//----------------------------------------------------------------------------------------------------------------------

void Schema::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {
    for (const auto& rule : rules_) {
        rule->expand(request, visitor);
    }
}

std::vector<Key> Schema::expandDatabase(const metkit::mars::MarsRequest& request) const {
    std::vector<Key> result;

    for (const auto& rule : rules_) {
        const auto keys = rule->findMatchingKeys(request);
        result.insert(result.end(), keys.begin(), keys.end());
    }

    return result;
}

void Schema::expand(const Key& field, WriteVisitor& visitor) const {

    visitor.rule(nullptr);  // reset to no rule so we verify that we pick at least one

    for (const auto& rule : rules_) {
        if (rule->expand(field, visitor)) {
            break;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

void Schema::matchDatabase(const Key& dbKey, std::map<Key, const Rule*>& result, const char* missing) const {
    for (const auto& rule : rules_) {
        if (auto key = rule->findMatchingKey(dbKey, missing)) {
            result[std::move(*key)] = rule.get();
        }
    }
}

void Schema::matchDatabase(const metkit::mars::MarsRequest& request, std::map<Key, const Rule*>& result,
                           const char* missing) const {
    for (const auto& rule : rules_) {
        const auto keys = rule->findMatchingKeys(request, missing);
        for (const auto& k : keys) {
            result[k] = rule.get();
        }
    }
}

std::optional<Key> Schema::matchDatabase(const std::string& fingerprint) const {

    const auto values = eckit::Tokenizer(":", true).tokenize(fingerprint);

    for (const auto& rule : rules_) {
        if (auto found = rule->findMatchingKey(values)) {
            return found;
        }
    }

    return {};
}

//----------------------------------------------------------------------------------------------------------------------

void Schema::load(const eckit::PathName& path, const bool replace) {

    path_ = path;

    LOG_DEBUG_LIB(LibFdb5) << "Loading FDB rules from " << path << std::endl;

    std::ifstream in(path.localPath());
    if (!in) {
        auto ex = eckit::CantOpenFile(path);
        ex.dumpStackTrace();
        throw ex;
    }

    load(in, replace);
}

void Schema::load(std::istream& s, const bool replace) {

    if (replace) {
        clear();
    }

    SchemaParser(s).parse(rules_, registry_);

    check();
}

//----------------------------------------------------------------------------------------------------------------------

void Schema::clear() {
    rules_.clear();
}

void Schema::dump(std::ostream& s) const {
    registry_.dump(s);
    for (const auto& rule : rules_) {
        rule->dump(s);
        s << '\n';
    }
}

void Schema::check() {
    for (auto& rule : rules_) {
        rule->registry_.updateParent(registry_);
        rule->updateParent(nullptr);
    }
}

void Schema::print(std::ostream& out) const {
    out << "Schema[path=" << path_ << "]";
}

const Type& Schema::lookupType(const std::string& keyword) const {
    return registry_.lookupType(keyword);
}

bool Schema::empty() const {
    return rules_.empty();
}

const std::string& Schema::path() const {
    return path_;
}

const TypesRegistry& Schema::registry() const {
    return registry_;
}

std::ostream& operator<<(std::ostream& out, const Schema& schema) {
    schema.print(out);
    return out;
}

//----------------------------------------------------------------------------------------------------------------------
// REGISTRY

SchemaRegistry& SchemaRegistry::instance() {
    static SchemaRegistry me;
    return me;
}

const Schema& SchemaRegistry::add(const eckit::PathName& path, Schema* schema) {
    ASSERT(schema);
    schemas_[path] = std::unique_ptr<Schema>(schema);
    return *schemas_[path];
}

const Schema& SchemaRegistry::get(const eckit::PathName& path) {
    std::lock_guard lock(m_);

    auto iter = schemas_.find(path);

    if (iter == schemas_.end()) {
        bool done = false;

        std::tie(iter, done) = schemas_.emplace(path, std::make_unique<Schema>(path));

        ASSERT(done);
    }

    ASSERT(iter->second);
    return *iter->second;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
