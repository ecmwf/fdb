/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
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
#include "fdb5/database/KeyChain.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema() : registry_(new TypesRegistry()) {

}

Schema::Schema(const eckit::PathName &path) : registry_(new TypesRegistry()) {
    load(path);
}

Schema::Schema(std::istream& s) : registry_(new TypesRegistry()) {
    load(s);
}

Schema::~Schema() {
    clear();
}

const Rule*  Schema::ruleFor(const Key& dbKey, const Key& idxKey) const {
    KeyChain keys {dbKey, idxKey};

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        const Rule* r = (*i)->ruleFor(keys , 0);
        if (r) {
            return r;
        }
    }
    return 0;
}

std::vector<const Rule*> Schema::getRules(const Key& key) const {

    for (const auto* rule : rules_) {
        if (rule->match(key)) { return rule->subRulesView(); }
    }

    return {};
}

void Schema::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {
    for (const auto* rule : rules_) { rule->expand(request, visitor); }
}

std::vector<Key> Schema::expandDatabase(const metkit::mars::MarsRequest& request) const {
    std::vector<Key> result;

    for (const auto* rule : rules_) {
        const auto keys = rule->findMatchingKeys(request);
        result.insert(result.end(), keys.begin(), keys.end());
    }

    return result;
}

void Schema::expand(const Key& field, WriteVisitor& visitor) const {

    visitor.rule(nullptr);  // reset to no rule so we verify that we pick at least one

    for (const auto* rule : rules_) {
        if (rule->expand(field, visitor)) { break; }
    }
}

void Schema::matchFirstLevel(const Key& dbKey, std::set<Key>& result, const char* missing) const {
    for (auto* rule : rules_) {
        if (auto key = rule->findMatchingKey(dbKey, missing)) { result.emplace(std::move(*key)); }
    }
}

void Schema::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    for (auto* rule : rules_) {
        const auto keys = rule->findMatchingKeys(request, missing);
        result.insert(keys.begin(), keys.end());
    }
}

std::unique_ptr<Key>
Schema::matchDatabase(const std::string &fingerprint) const {

  const auto values = eckit::Tokenizer(":", true).tokenize(fingerprint);

  for (const auto *rule : rules_) {
    if (auto found = rule->findMatchingKey(values)) {
      return found;
    }
  }

  return {};
}

void Schema::load(const eckit::PathName &path, bool replace) {

    path_ = path;

    LOG_DEBUG_LIB(LibFdb5) << "Loading FDB rules from " << path << std::endl;

    std::ifstream in(path.localPath());
    if (!in)
        throw eckit::CantOpenFile(path);

    load(in, replace);
}

void Schema::load(std::istream& s, bool replace) {

    if (replace) {
        clear();
    }

    SchemaParser parser(s);

    parser.parse(*this, rules_, *registry_);

    check();
}

void Schema::clear() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Schema::dump(std::ostream &s) const {
    registry_->dump(s);
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Schema::check() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        /// @todo print offending rule in meaningful message
        ASSERT((*i)->depth() == 3);
        (*i)->registry_->updateParent(registry_);
        (*i)->updateParent(0);
    }
}

void Schema::print(std::ostream &out) const {
    out << "Schema[path=" << path_ << "]";
}

const Type &Schema::lookupType(const std::string &keyword) const {
    return registry_->lookupType(keyword);
}


bool Schema::empty() const {
    return rules_.empty();
}

const std::string &Schema::path() const {
    return path_;
}

const std::shared_ptr<TypesRegistry> Schema::registry() const {
    return registry_;
}


std::ostream &operator<<(std::ostream &s, const Schema &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

SchemaRegistry& SchemaRegistry::instance() {
    static SchemaRegistry me;
    return me;
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

} // namespace fdb5
