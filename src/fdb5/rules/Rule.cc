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
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

#include "eckit/types/Types.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/Key.h"
#include "fdb5/database/KeyChain.h"
#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// GRAPH

namespace {

class RuleGraph {
    struct RuleNode {
        explicit RuleNode(const std::string& keyword): keyword_ {keyword} { }

        std::string keyword_;

        eckit::StringList values_;
    };

public:  // types
    using value_type     = std::list<RuleNode>;
    using reference      = eckit::StringList&;
    using const_iterator = value_type::const_iterator;

public:  // methods
    reference push(const std::string& keyword) { return nodes_.emplace_back(keyword).values_; }

    std::size_t size() const { return nodes_.size(); }

    std::vector<Key> makeKeys(const std::shared_ptr<TypesRegistry>& registry) const {
        std::vector<Key> keys;

        if (!nodes_.empty()) {
            Key key(registry);
            visit(nodes_.begin(), key, keys);
        }

        return keys;
    }

private:  // methods
    // Recursive DFS (depth-first search) to generate all possible keys
    void visit(const_iterator iter, Key& key, std::vector<Key>& keys) const {

        if (iter == nodes_.end()) {
            keys.push_back(key);
            return;
        }

        const auto& [keyword, values] = *iter;

        auto next = iter;
        ++next;

        for (const auto& value : values) {
            key.push(keyword, value);
            visit(next, key, keys);
            key.pop(keyword);
        }
    }

private:  // members
    value_type nodes_;
};

}  // namespace

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
// MATCHING KEYS

std::unique_ptr<Key> Rule::findMatchingKey(const eckit::StringList& values) const {

    if (predicates_.empty()) { return {}; }

    ASSERT(values.size() >= predicates_.size());

    auto key = std::make_unique<Key>(registry_);

    for (auto iter = predicates_.begin(); iter != predicates_.end(); ++iter) {
        const auto* pred = *iter;

        const auto& keyword = pred->keyword();

        // 1-1 order between predicates and values
        const auto& value = values.at(iter - predicates_.begin());

        if (!pred->match(value)) { return {}; }

        key->push(keyword, value);
    }

    return key;
}

std::unique_ptr<Key> Rule::findMatchingKey(const Key& field) const {

    if (field.size() < predicates_.size()) { return {}; }

    auto key = std::make_unique<Key>(registry_);

    for (auto* pred : predicates_) {

        /// @note the key is constructed from the predicate
        if (!pred->match(field)) { return {}; }

        const auto& keyword = pred->keyword();

        key->push(keyword, pred->value(field));
    }

    return key;
}

std::unique_ptr<Key> Rule::findMatchingKey(const Key& field, const char* missing) const {

    auto key = std::make_unique<Key>(registry_);

    for (auto* pred : predicates_) {

        const auto& keyword = pred->keyword();

        if (field.find(keyword) == field.end()) {
            key->push(keyword, missing);
        } else if (pred->match(field)) {
            key->push(keyword, pred->value(field));
        } else {
            return {};
        }
    }

    return key;
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request, const char* missing) const {

    RuleGraph graph;

    for (const auto* pred : predicates_) {

        const auto& keyword = pred->keyword();

        auto& node = graph.push(keyword);

        if (!request.has(keyword)) {
            node.emplace_back(missing);
        } else {
            const auto& values = pred->values(request);

            for (const auto& value : values) {
                if (pred->match(value)) { node.emplace_back(value); }
            }

            if (node.empty()) { break; }
        }
    }

    /// @note request match all keys ?

    return graph.makeKeys(registry_);
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request) const {

    RuleGraph graph;

    for (const auto* pred : predicates_) {

        const auto& keyword = pred->keyword();

        const auto& values = pred->values(request);

        /// @note do we want to allow empty values?
        // if (values.empty() && pred->optional()) { values.push_back(pred->defaultValue()); }

        auto& node = graph.push(keyword);

        for (const auto& value : values) {
            if (pred->match(value)) { node.emplace_back(value); }
        }

        if (node.empty()) { break; }
    }

    if (graph.size() == predicates_.size()) { return graph.makeKeys(registry_); }

    return {};
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {

    RuleGraph graph;

    for (const auto* pred : predicates_) {

        const auto& keyword = pred->keyword();

        eckit::StringList values;
        visitor.values(request, keyword, *registry_, values);

        if (values.empty() && pred->optional()) { values.push_back(pred->defaultValue()); }

        auto& node = graph.push(keyword);

        for (const auto& value : values) {
            if (pred->match(value)) { node.emplace_back(value); }
        }

        if (node.empty()) { break; }
    }

    if (graph.size() == predicates_.size()) { return graph.makeKeys(registry_); }

    return {};
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: READ PATH

void Rule::expandDatum(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const {

    ASSERT(rules_.empty());

    for (auto& key : findMatchingKeys(request, visitor)) {

        full.pushFrom(key);

        visitor.selectDatum(key, full);

        full.popFrom(key);
    }
}

void Rule::expandIndex(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const {

    for (auto& key : findMatchingKeys(request, visitor)) {

        full.pushFrom(key);

        if (visitor.selectIndex(key, full)) {
            for (const auto* rule : rules_) { rule->expandDatum(request, visitor, full); }
        }

        full.popFrom(key);
    }
}

void Rule::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {

    for (auto& key : findMatchingKeys(request, visitor)) {

        if (visitor.selectDatabase(key, key)) {
            // (important) using the database's schema
            for (const auto* rule : visitor.databaseSchema().getRules(key)) {
                rule->expandIndex(request, visitor, key);
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------
// EXPAND: WRITE PATH

bool Rule::expandDatum(const Key& field, WriteVisitor& visitor, Key& full) const {

    ASSERT(rules_.empty());

    if (auto key = findMatchingKey(field)) {

        full.pushFrom(*key);

        if (visitor.rule()) {
            std::ostringstream oss;
            oss << "More than one rule matching " << full << " " << topRule() << " and " << visitor.rule()->topRule();
            throw eckit::SeriousBug(oss.str());
        }

        if (visitor.selectDatum(*key, full)) {
            visitor.rule(this);
            static const bool matchFirstFdbRule = eckit::Resource<bool>("matchFirstFdbRule", true);
            if (matchFirstFdbRule) { return true; }
        }

        full.popFrom(*key);
    }

    return false;
}

bool Rule::expandIndex(const Key& field, WriteVisitor& visitor, Key& full) const {

    if (auto key = findMatchingKey(field)) {

        full.pushFrom(*key);

        if (visitor.selectIndex(*key, full)) {
            for (const auto* rule : rules_) {
                if (rule->expandDatum(field, visitor, full)) { return true; }
            }
        }

        full.popFrom(*key);
    }

    return false;
}

bool Rule::expand(const Key& field, WriteVisitor& visitor) const {

    if (auto key = findMatchingKey(field)) {

        if (visitor.selectDatabase(*key, *key)) {
            // (important) using the database's schema
            for (const auto* rule : visitor.databaseSchema().getRules(*key)) {
                if (rule->expandIndex(field, visitor, *key)) { return true; }
            }
        }
    }

    return false;
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
