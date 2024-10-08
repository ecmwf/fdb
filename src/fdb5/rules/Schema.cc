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
#include <memory>
#include <mutex>

#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema() = default;

Schema::Schema(const eckit::PathName &path) {
    load(path);
}

Schema::Schema(std::istream& s) {
    load(s);
}

Schema::~Schema() {
    clear();
}

const Rule*  Schema::ruleFor(const Key& dbKey, const Key& idxKey) const {
    std::vector<Key> keys;
    keys.push_back(dbKey);
    keys.push_back(idxKey);

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        const Rule* r = (*i)->ruleFor(keys , 0);
        if (r) {
            return r;
        }
    }
    return 0;
}

void Schema::expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const {
    TypedKey fullComputedKey{registry()};
    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});

    for (Rule* r : rules_) {
		r->expand(request, visitor, 0, keys, fullComputedKey);
    }
}

void Schema::expand(const Key& field, WriteVisitor &visitor) const {
    TypedKey fullComputedKey{registry()};
    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});

    visitor.rule(0); // reset to no rule so we verify that we pick at least one

    for (Rule* r : rules_) {
        r->expand(field, visitor, 0, keys, fullComputedKey);
    }
}

void Schema::expandSecond(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* r : rules_) {
        if (r->match(dbKey)) {
            dbRule = r;
            break;
        }
    }
    ASSERT(dbRule);

    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});
    TypedKey fullComputedKey = keys[0] = TypedKey{dbKey, registry()};

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(request, visitor, 1, keys, fullComputedKey);
    }
}

void Schema::expandSecond(const Key& field, WriteVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* r : rules_) {
        if (r->match(dbKey)) {
            dbRule = r;
            break;
        }
    }
    ASSERT(dbRule);

    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});
    TypedKey fullComputedKey = keys[0] = TypedKey{dbKey, registry()};

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(field, visitor, 1, keys, fullComputedKey);
    }
}

bool Schema::expandFirstLevel(const metkit::mars::MarsRequest& request, TypedKey& result) const {
    bool found = false;
    for (const Rule* rule : rules_) {
        rule->expandFirstLevel(request, result, found);
        if (found) {
            result.registry(rule->registry());
            break;
        }
    }
    return found;
}

void Schema::matchFirstLevel(const Key& dbKey,  std::set<Key> &result, const char* missing) const {
    for (const Rule* rule : rules_) {
        rule->matchFirstLevel(dbKey, result, missing);
    }
}

void Schema::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    for (const Rule* rule : rules_) {
        rule->matchFirstLevel(request, result, missing);
    }
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

    parser.parse(*this, rules_, registry_);

    check();
}

void Schema::clear() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Schema::dump(std::ostream &s) const {
    registry_.dump(s);
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Schema::check() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        /// @todo print offending rule in meaningful message
        ASSERT((*i)->depth() == 3);
        (*i)->registry_.updateParent(registry_);
        (*i)->updateParent(0);
    }
}

void Schema::print(std::ostream &out) const {
    out << "Schema[path=" << path_ << "]";
}

const Type &Schema::lookupType(const std::string &keyword) const {
    return registry_.lookupType(keyword);
}


bool Schema::empty() const {
    return rules_.empty();
}

const std::string &Schema::path() const {
    return path_;
}

const TypesRegistry& Schema::registry() const {
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
    std::lock_guard<std::mutex> lock(m_);
    auto it = schemas_.find(path);
    if (it != schemas_.end()) {
        return *it->second;
    }

    Schema* p = new Schema(path);
    ASSERT(p);
    schemas_[path] = std::unique_ptr<Schema>(p);
    return *schemas_[path];
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
