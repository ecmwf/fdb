/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rules/Rule.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/KeyChain.h"
#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rule::Rule(const Schema& schema, const std::size_t line, std::vector<Predicate*>&& predicates,
           std::vector<Rule*>&& rules, const std::map<std::string, std::string>& types):
    schema_ {schema},
    line_ {line},
    predicates_ {std::move(predicates)},
    rules_ {std::move(rules)},
    registry_ {std::make_shared<TypesRegistry>()} {
    for (const auto& [keyword, type] : types) { registry_->addType(keyword, type); }
}

Rule::~Rule() {
    //    eckit::Log::info() << " === Deleting rule " << this << std::endl;
    //    eckit::Log::info() << eckit::BackTrace::dump() << std::endl;
    for (auto& predicate : predicates_) {
        delete predicate;
        predicate = nullptr;
    }
    for (auto& rule : rules_) {
        delete rule;
        rule = nullptr;
    }
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: READ

void Rule::expand(const metkit::mars::MarsRequest& request, std::vector<Predicate*>::const_iterator cur,
                  const std::size_t depth, KeyChain& keys, Key& full, ReadVisitor& visitor) const {

    ASSERT(depth < 3);

    Key& key = keys[depth];

    // eckit::Log::info() << "depth: " << depth << " - predicates: " << predicates_.size() << " cur "
    //                    << (cur - predicates_.begin()) << std::endl;

    if (cur == predicates_.end()) {

        key.registry(registry());

        // eckit::Log::info() << "RQ Setting rule, depth: " << depth << " : " << this << std::endl;

        if (depth == 0) {
            if (visitor.selectDatabase(key, full)) {
                // ASSERT(key == full);
                // Here we recurse on the database's schema (rather than the master schema)
                // see https://jira.ecmwf.int/browse/FDB-90
                visitor.databaseSchema().expandIndex(request, visitor, key);
            };
        } else if (depth == 1) {
            if (visitor.selectIndex(key, full)) {
                for (auto* rule : rules_) { rule->expandDatum(request, visitor, keys, full); }
            }
        } else if (depth == 2) {
            ASSERT(rules_.empty());
            visitor.selectDatum(key, full);
        }

        return;
    }

    const auto* pred = *cur;

    const std::string& keyword = pred->keyword();

    eckit::StringList values;
    visitor.values(request, keyword, *registry_, values);

    // eckit::Log::info() << "keyword " << keyword << " values " << values << std::endl;

    if (values.empty() && pred->optional()) { values.push_back(pred->defaultValue()); }

    auto next = cur;
    ++next;

    for (const auto& value : values) {

        key.push(keyword, value);
        full.push(keyword, value);

        if (pred->match(key)) { expand(request, next, depth, keys, full, visitor); }

        full.pop(keyword);
        key.pop(keyword);
    }
}

void Rule::expandDatabase(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, KeyChain& keys,
                          Key& full) const {
    expand(request, predicates_.begin(), 0, keys, full, visitor);
}

void Rule::expandIndex(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, KeyChain& keys, Key& full) const {
    expand(request, predicates_.begin(), 1, keys, full, visitor);
}

void Rule::expandDatum(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, KeyChain& keys, Key& full) const {
    expand(request, predicates_.begin(), 2, keys, full, visitor);
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: WRITE

void Rule::expand(const Key& field, std::vector<Predicate*>::const_iterator cur, const std::size_t depth,
                  KeyChain& keys, Key& full, WriteVisitor& visitor) const {

    static bool matchFirstFdbRule = eckit::Resource<bool>("matchFirstFdbRule", true);

    if (matchFirstFdbRule && visitor.rule()) { return; }

    ASSERT(depth < 3);

    Key& key = keys[depth];

    if (cur == predicates_.end()) {

        key.registry(registry());

        if (depth == 0) {
            if (visitor.prev_[0] != key) {
                visitor.selectDatabase(key, full);
                visitor.prev_[0] = key;
                visitor.prev_[1] = Key();
            }
            // Here we recurse on the database's schema (rather than the master schema)
            // see https://jira.ecmwf.int/browse/FDB-90
            visitor.databaseSchema().expandSecond(field, visitor, key);
        } else if (depth == 1) {
            if (visitor.prev_[1] != key) {
                visitor.selectIndex(key, full);
                visitor.prev_[1] = key;
            }
            for (auto* rule : rules_) { rule->expandDatum(field, visitor, keys, full); }
        } else if (depth == 2) {
            ASSERT(rules_.empty());
            // if (rules_.empty()) {
            if (visitor.rule()) {
                std::ostringstream oss;
                oss << "More than one rule matching " << keys[0] << ", " << keys[1] << ", " << keys[2] << " "
                    << topRule() << " and " << visitor.rule()->topRule();
                throw eckit::SeriousBug(oss.str());
            }
            visitor.rule(this);
            visitor.selectDatum(key, full);
            // }
        }

        return;
    }

    const auto* pred    = *cur;
    const auto& keyword = pred->keyword();
    const auto& value   = pred->value(field);

    key.push(keyword, value);
    full.push(keyword, value);

    auto next = cur;
    ++next;

    if (pred->match(key)) { expand(field, next, depth, keys, full, visitor); }

    full.pop(keyword);
    key.pop(keyword);
}

void Rule::expandDatabase(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const {
    expand(field, predicates_.begin(), 0, keys, full, visitor);
}

void Rule::expandIndex(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const {
    expand(field, predicates_.begin(), 1, keys, full, visitor);
}

void Rule::expandDatum(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const {
    expand(field, predicates_.begin(), 2, keys, full, visitor);
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: FIRST LEVEL (KEY)

void Rule::expandFirstLevel(const Key& dbKey, std::vector<Predicate*>::const_iterator cur, Key& key, bool& found) const {

    if (cur == predicates_.end()) {
        found = true;
        key.registry(registry());
        return;
    }

    auto next = cur;
    ++next;

    const auto* pred    = *cur;
    const auto& keyword = pred->keyword();
    const auto& value   = pred->value(dbKey);

    key.push(keyword, value);

    if (pred->match(key)) { expandFirstLevel(dbKey, next, key, found); }

    if (!found) { key.pop(keyword); }
}

void Rule::expandFirstLevel(const Key& dbKey, Key& result, bool& found) const {
    expandFirstLevel(dbKey, predicates_.begin(), result, found);
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: FIRST LEVEL (REQUEST)

void Rule::expandFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate*>::const_iterator cur,
                            std::vector<Key>& results, Key& key, bool& found) const {

    if (cur == predicates_.end()) {
        found = true;
        key.registry(registry());
        results.push_back(key);
        return;
    }

    auto next = cur;
    ++next;

    const auto* pred    = *cur;
    const auto& keyword = pred->keyword();
    const auto& values  = pred->values(request);

    // Gives a unique expansion --> only considers the first of the values suggested.
    /// @todo Consider the broader case.

    for (const auto& value : values) {
        key.push(keyword, value);

        if (pred->match(key)) { expandFirstLevel(request, next, results, key, found); }

        if (found) { return; }

        key.pop(keyword);
    }
}

void Rule::expandFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Key>& results, bool& found) const {
    Key tmp;
    expandFirstLevel(request, predicates_.begin(), results, tmp, found);
}

//----------------------------------------------------------------------------------------------------------------------
// MATCH: FIRST LEVEL (KEY)

void Rule::matchFirstLevel(const Key& dbKey, std::vector<Predicate*>::const_iterator cur, Key& key,
                           std::set<Key>& result, const char* missing) const {

    if (cur == predicates_.end()) {
        if (key.match(dbKey)) { result.insert(key); }
        return;
    }

    auto next = cur;
    ++next;

    const auto* pred    = *cur;
    const auto& keyword = pred->keyword();

    if (dbKey.find(keyword) == dbKey.end()) {
        key.push(keyword, missing);
        matchFirstLevel(dbKey, next, key, result, missing);
    } else {
        const auto& value = pred->value(dbKey);

        key.push(keyword, value);

        if (pred->match(key)) { matchFirstLevel(dbKey, next, key, result, missing); }
    }

    key.pop(keyword);
}

void Rule::matchFirstLevel(const Key& dbKey, std::set<Key>& result, const char* missing) const {
    Key tmp;
    matchFirstLevel(dbKey, predicates_.begin(), tmp, result, missing);
}

//----------------------------------------------------------------------------------------------------------------------
// MATCH: FIRST LEVEL (REQUEST)

void Rule::matchFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate*>::const_iterator cur,
                           Key& key, std::set<Key>& result, const char* missing) const {

    if (cur == predicates_.end()) {
        // if (key.match(request)) {
        result.insert(key);
        // }
        return;
    }

    auto next = cur;
    ++next;

    const auto* pred    = *cur;
    const auto& keyword = pred->keyword();

    if (request.has(keyword)) {

        const auto& values = pred->values(request);

        for (const std::string& value : values) {
            key.push(keyword, value);
            if (pred->match(key)) { matchFirstLevel(request, next, key, result, missing); }
            key.pop(keyword);
        }
    } else {
        key.push(keyword, missing);
        matchFirstLevel(request, next, key, result, missing);
        key.pop(keyword);
    }
}

void Rule::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    Key tmp(registry());
    matchFirstLevel(request, predicates_.begin(), tmp, result, missing);
}

//----------------------------------------------------------------------------------------------------------------------

bool Rule::match(const Key &key) const {
    for (std::vector<Predicate *>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        if (!(*i)->match(key)) {
            return false;
        }
    }
    return true;
}

// Find the first rule that matches a list of keys
const Rule* Rule::ruleFor(const KeyChain& keys, const std::size_t depth) const {

    if (depth == keys.size() - 1) { return this; }

    if (match(keys[depth])) {

        for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
            const Rule *r = (*i)->ruleFor(keys, depth + 1);
            if (r) {
                return r;
            }
        }
    }
    return 0;
}

bool Rule::tryFill(Key& key, const eckit::StringList& values) const {
    // See FDB-103. This is a hack to work around the indexing abstraction
    // being leaky.
    //
    // i) Indexing is according to a colon-separated string of values
    // ii) This string of values is passed to, and split in, the constructor
    //     of Key().
    // iii) The constructor of Key does not know what these values correspond
    //      to, so calls this function to map them to the Predicates.
    //
    // This whole process really ought to take place inside of (Toc)-Index,
    // such that a Key() is returned, and any kludgery is contained there.
    // But that is too large a change to safely make this close to
    // operational switchover day.
    //
    // --> HACK.
    // --> Stick a plaster over the symptom.

    ASSERT(values.size() >= predicates_.size()); // Should be equal, except for quantile (FDB-103)
    ASSERT(values.size() <= predicates_.size() + 1);

    auto it_value = values.begin();
    auto it_pred = predicates_.begin();

    for (; it_pred != predicates_.end() && it_value != values.end(); ++it_pred, ++it_value) {

        if (values.size() == (predicates_.size() + 1) && (*it_pred)->keyword() == "quantile") {
            std::string actualQuantile = *it_value;
            ++it_value;
            if (it_value == values.end()) { return false; }
            actualQuantile += std::string(":") + (*it_value);
            (*it_pred)->fill(key, actualQuantile);
        } else {
            (*it_pred)->fill(key, *it_value);
        }
    }

    // Check that everything is exactly consumed
    if (it_value != values.end()) { return false; }
    if (it_pred != predicates_.end()) { return false; }
    return true;
}

void Rule::fill(Key& key, const eckit::StringList& values) const {
    // FDB-103 - see comment in fill re quantile

    ASSERT(values.size() >= predicates_.size());  // Should be equal, except for quantile (FDB-103)
    ASSERT(values.size() <= predicates_.size() + 1);
    ASSERT(tryFill(key, values));
}

//----------------------------------------------------------------------------------------------------------------------

void Rule::dump(std::ostream& s, const std::size_t depth) const {
    s << "[";
    const char *sep = "";
    for (std::vector<Predicate *>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        s << sep;
        (*i)->dump(s, *registry_);
        sep = ",";
    }

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s, depth + 1);
    }
    s << "]";
}

size_t Rule::depth() const {
    size_t result = 0;
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        result = std::max(result, (*i)->depth());
    }
    return result + 1;
}

void Rule::updateParent(const Rule *parent) {
    parent_ = parent;
    if (parent) {
        registry_->updateParent(parent_->registry_);
    }
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->updateParent(this);
    }
}

const std::shared_ptr<TypesRegistry> Rule::registry() const {
    return registry_;
}

void Rule::print(std::ostream &out) const {
    out << "Rule[line=" << line_ << ", ]";
}

const Rule &Rule::topRule() const {
    if (parent_) {
        return parent_->topRule();
    } else {
        return *this;
    }
}

const Schema &Rule::schema() const {
    return schema_;
}

void Rule::check(const Key& key) const {
    for (const auto& pred : predicates_ ) {
        auto k = key.find(pred->keyword());
        if (k != key.end()) {
            const std::string& value = (*k).second;
            const Type& type = registry_->lookupType(pred->keyword());
            if (value != type.tidy(pred->keyword(), value)) {
                std::stringstream ss;
                ss << "Rule check - metadata not valid (not in canonical form) - found: ";
                ss << pred->keyword() << "=" << value << " - expecting " << type.tidy(pred->keyword(), value) << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }
        }
    }
    if (parent_ != nullptr) {
        parent_->check(key);
    }
}

const std::vector<Predicate*>& Rule::predicates() const {
    return predicates_;
}

const std::vector<Rule*>& Rule::subRules() const {
    return rules_;
}

std::ostream &operator<<(std::ostream &s, const Rule &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
