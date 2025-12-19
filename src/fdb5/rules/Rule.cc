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
#include <iostream>
#include <list>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Tokenizer.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/BaseKey.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// GRAPH

namespace {

class RuleGraph {
    struct RuleNode {

        RuleNode(const RuleNode&) = delete;
        RuleNode& operator=(const RuleNode&) = delete;
        RuleNode(RuleNode&&) = delete;
        RuleNode& operator=(RuleNode&&) = delete;
        ~RuleNode() = default;

        explicit RuleNode(const std::string& keyword) : keyword_{keyword} {}

        std::string keyword_;

        eckit::StringList values_;
    };

public:  // types

    using value_type = std::list<RuleNode>;
    using reference = eckit::StringList&;
    using const_iterator = value_type::const_iterator;

public:  // methods

    reference push(const std::string& keyword) { return nodes_.emplace_back(keyword).values_; }

    std::size_t size() const { return nodes_.size(); }

    std::vector<Key> makeKeys() const {
        std::set<Key> seen;
        std::vector<Key> keys;

        if (!nodes_.empty()) {
            Key key;
            visit(nodes_.begin(), key, seen, keys);
        }

        return keys;
    }

    void canonicalise(const TypesRegistry& registry) {
        for (auto& [keyword, values] : nodes_) {
            const auto& type = registry.lookupType(keyword);
            for (auto& value : values) {
                if (!value.empty()) {
                    value = type.toKey(value);
                }
            }
            keyword = type.alias();
        }
    }

private:  // methods

    // Recursive DFS (depth-first search) to generate all possible keys
    void visit(const_iterator iter, Key& key, std::set<Key>& seen, std::vector<Key>& keys) const {

        if (iter == nodes_.end()) {
            auto it = seen.find(key);
            if (it == seen.end()) {
                seen.insert(key);
                keys.push_back(key);
            }
            return;
        }

        const auto& [keyword, values] = *iter;

        auto next = iter;
        ++next;

        for (const auto& value : values) {
            key.push(keyword, value);
            visit(next, key, seen, keys);
            key.pop(keyword);
        }
    }

private:  // members

    value_type nodes_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// RULE

eckit::ClassSpec RuleDatum::classSpec_ = {&Rule::classSpec(), "RuleDatum"};
eckit::Reanimator<RuleDatum> RuleDatum::reanimator_;

eckit::ClassSpec RuleIndex::classSpec_ = {&Rule::classSpec(), "RuleIndex"};
eckit::Reanimator<RuleIndex> RuleIndex::reanimator_;

eckit::ClassSpec RuleDatabase::classSpec_ = {&Rule::classSpec(), "RuleDatabase"};
eckit::Reanimator<RuleDatabase> RuleDatabase::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

Rule::Rule(const std::size_t line, Predicates& predicates, const eckit::StringDict& types) :
    line_{line}, predicates_{std::move(predicates)} {
    for (const auto& [keyword, type] : types) {
        registry_.addType(keyword, type);
    }
}

void Rule::decode(eckit::Stream& stream) {
    size_t numPred = 0;

    registry_.decode(stream);
    stream >> line_;
    stream >> numPred;

    predicates_.reserve(numPred);
    for (size_t i = 0; i < numPred; ++i) {
        predicates_.emplace_back(eckit::Reanimator<Predicate>::reanimate(stream));
    }
}

void Rule::encode(eckit::Stream& out) const {
    registry_.encode(out);
    out << line_;
    out << predicates_.size();
    for (const auto& pred : predicates_) {
        out << *pred;
    }
}

size_t Rule::size() const {
    return predicates_.size();
}

//----------------------------------------------------------------------------------------------------------------------
// MATCHING KEYS

std::optional<Key> Rule::findMatchingKey(const Key& field) const {

    if (field.size() < predicates_.size()) {
        return {};
    }

    TypedKey key(registry_);

    for (const auto& pred : predicates_) {

        /// @note the key is constructed from the predicate
        if (!pred->match(field)) {
            return {};
        }

        const auto& keyword = pred->keyword();

        key.push(keyword, pred->value(field));
    }

    return key.canonical();
}

std::optional<Key> Rule::findMatchingKey(const eckit::StringList& values) const {

    if (predicates_.empty()) {
        return {};
    }

    ASSERT(values.size() >= predicates_.size());

    TypedKey key(registry_);

    for (auto iter = predicates_.begin(); iter != predicates_.end(); ++iter) {
        const auto& pred = *iter;

        const auto& keyword = pred->keyword();

        /// @note 1-1 order between predicates and values
        const auto& value = values.at(iter - predicates_.begin());

        if (!pred->match(value)) {
            return {};
        }

        key.push(keyword, value);
    }

    return key.canonical();
}

std::optional<Key> Rule::findMatchingKey(const Key& field, const char* missing) const {

    Key key;

    for (const auto& pred : predicates_) {

        const auto& keyword = pred->keyword();

        if (const auto [iter, found] = field.find(keyword); found) {
            if (pred->match(iter->second)) {
                key.push(keyword, iter->second);
            }
            else {
                return {};
            }
        }
        else {
            key.push(keyword, missing);
        }
    }

    return key;
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request, const char* missing) const {

    RuleGraph graph;

    for (const auto& pred : predicates_) {

        const auto& keyword = pred->keyword();

        auto& node = graph.push(keyword);

        if (!request.has(keyword)) {
            node.emplace_back(missing);
        }
        else {
            const auto& values = pred->values(request);

            for (const auto& value : values) {
                if (pred->match(value)) {
                    node.emplace_back(value);
                }
            }

            if (node.empty()) {
                break;
            }
        }
    }

    /// @todo activate this
    graph.canonicalise(registry_);

    return graph.makeKeys();
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request) const {

    RuleGraph graph;

    for (const auto& pred : predicates_) {

        const auto& keyword = pred->keyword();
        const auto& type = registry_.lookupType(keyword);


        const auto& values = pred->values(request);

        /// @note do we want to allow empty values?
        // if (values.empty() && pred->optional()) { values.push_back(pred->defaultValue()); }

        auto& node = graph.push(keyword);

        for (const auto& value : values) {
            if (pred->match(value)) {
                node.emplace_back(value);
            }
        }

        if (node.empty()) {
            return {};
        }
    }

    graph.canonicalise(registry_);

    return graph.makeKeys();
}

std::vector<Key> Rule::findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {

    RuleGraph graph;

    for (const auto& pred : predicates_) {

        const auto& keyword = pred->keyword();
        const auto& type = registry_.lookupType(keyword);

        // performance optimization to avoid calling values() on visitor
        if (!pred->optional() && request.countValues(keyword) == 0) {
            return {};
        }

        eckit::StringList values;
        visitor.values(request, keyword, registry_, values);

        if (values.empty() && pred->optional()) {
            values.push_back(pred->defaultValue());
        }

        auto& node = graph.push(keyword);

        for (const auto& value : values) {
            if (pred->match(value)) {
                node.emplace_back(value);
            }
        }

        if (node.empty()) {
            return {};
        }
    }

    graph.canonicalise(registry_);

    auto out = graph.makeKeys();

    LOG_DEBUG_LIB(LibFdb5) << "findMatchingKeys  " << request << " ==> ";
    std::string sep;
    for (const auto& k : out) {
        LOG_DEBUG_LIB(LibFdb5) << sep << k;
        sep = " | ";
    }
    LOG_DEBUG_LIB(LibFdb5) << std::endl;

    return out;
}

//----------------------------------------------------------------------------------------------------------------------

bool Rule::match(const Key& key) const {
    for (const auto& pred : predicates_) {
        if (!pred->match(key)) {
            return false;
        }
    }
    return true;
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

    ASSERT(values.size() >= predicates_.size());  // Should be equal, except for quantile (FDB-103)
    ASSERT(values.size() <= predicates_.size() + 1);

    auto it_value = values.begin();
    auto it_pred = predicates_.begin();

    for (; it_pred != predicates_.end() && it_value != values.end(); ++it_pred, ++it_value) {

        if (values.size() == (predicates_.size() + 1) && (*it_pred)->keyword() == "quantile") {
            std::string actualQuantile = *it_value;
            ++it_value;
            if (it_value == values.end()) {
                return false;
            }
            actualQuantile += std::string(":") + (*it_value);
            (*it_pred)->fill(key, actualQuantile);
        }
        else {
            (*it_pred)->fill(key, *it_value);
        }
    }

    // Check that everything is exactly consumed
    if (it_value != values.end()) {
        return false;
    }
    if (it_pred != predicates_.end()) {
        return false;
    }
    return true;
}

void Rule::fill(Key& key, const eckit::StringList& values) const {
    // FDB-103 - see comment in fill re quantile
    ASSERT(tryFill(key, values));
}

Key Rule::makeKey(const std::string& keyFingerprint) const {
    Key key;

    /// @note assumed keyFingerprint is canonical
    const auto values = eckit::Tokenizer(":", true).tokenize(keyFingerprint);

    fill(key, values);

    return key;
}

//----------------------------------------------------------------------------------------------------------------------

void Rule::dump(std::ostream& out) const {
    out << "[";

    const char* sep = "";
    for (const auto& pred : predicates_) {
        out << sep;
        pred->dump(out, registry_);
        sep = ",";
    }

    dumpChildren(out);

    out << "]";
}

void Rule::updateParent(const Rule* parent) {
    parent_ = parent;
    if (parent) {
        registry_.updateParent(parent_->registry_);
    }
}

const TypesRegistry& Rule::registry() const {
    return registry_;
}

void Rule::print(std::ostream& out) const {
    out << type() << "[line=" << line_ << ",predicates=[";
    std::string sep;
    for (const auto& p : predicates_) {
        out << sep << *p;
        sep = ",";
    }
    out << "]]";
}

const Rule& Rule::parent() const {
    ASSERT(parent_);
    return *parent_;
}

bool Rule::isTopRule() const {
    return parent_ == nullptr;
}

const Rule& Rule::topRule() const {
    return parent_ ? parent_->topRule() : *this;
}

void Rule::check(const Key& key) const {
    for (const auto& pred : predicates_) {

        const auto& keyword = pred->keyword();

        if (const auto [iter, found] = key.find(keyword); found) {
            const auto& value = iter->second;
            const auto& tidyValue = registry().lookupType(keyword).tidy(value);
            if (value != tidyValue) {
                std::ostringstream oss;
                oss << "Rule check - metadata not valid (not in canonical form) - found: ";
                oss << keyword << "=" << value << " - expecting " << tidyValue << '\n';
                throw eckit::UserError(oss.str(), Here());
            }
        }
    }

    if (parent_) {
        parent_->check(key);
    }
}

std::ostream& operator<<(std::ostream& out, const Rule& rule) {
    rule.print(out);
    return out;
}

//----------------------------------------------------------------------------------------------------------------------
// RULE DATUM

RuleDatum::RuleDatum(eckit::Stream& stream) : Rule() {
    decode(stream);
    size_t numRules;
    stream >> numRules;
    ASSERT(numRules == 0);
}

void RuleDatum::encode(eckit::Stream& out) const {
    Rule::encode(out);
    out << 0ul;
}

void RuleDatum::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const {

    for (const auto& key : findMatchingKeys(request, visitor)) {

        full.pushFrom(key);

        visitor.selectDatum(key, full);

        full.popFrom(key);
    }
}

bool RuleDatum::expand(const Key& field, WriteVisitor& visitor, Key& full) const {

    if (const auto key = findMatchingKey(field)) {

        full.pushFrom(*key);

        if (visitor.rule()) {
            std::ostringstream oss;
            oss << "More than one rule matching " << full << " " << topRule() << " and " << visitor.rule()->topRule();
            throw eckit::SeriousBug(oss.str());
        }

        if (visitor.selectDatum(*key, full)) {
            visitor.rule(this);
            static const bool matchFirstFdbRule = eckit::Resource<bool>("matchFirstFdbRule", true);
            if (matchFirstFdbRule) {
                return true;
            }
        }

        full.popFrom(*key);
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------
// RULE INDEX

RuleIndex::RuleIndex(const std::size_t line, Predicates& predicates, const eckit::StringDict& types, Child rule) :
    Rule(line, predicates, types), rule_{std::move(rule)} {}

RuleIndex::RuleIndex(eckit::Stream& stream) : Rule() {
    decode(stream);

    size_t numRules;
    stream >> numRules;
    ASSERT(numRules == 1);

    rule_.reset(new RuleDatum(stream));
}

void RuleIndex::encode(eckit::Stream& out) const {
    Rule::encode(out);
    ASSERT(rule_);
    out << (size_t)1;
    rule_->encode(out);
}

void RuleIndex::updateParent(const Rule* parent) {
    Rule::updateParent(parent);
    rule_->updateParent(this);
}

void RuleIndex::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const {
    for (const auto& key : findMatchingKeys(request, visitor)) {
        full.pushFrom(key);
        if (visitor.selectIndex(key)) {
            rule_->expand(request, visitor, full);
        }
        full.popFrom(key);
    }
}

bool RuleIndex::expand(const Key& field, WriteVisitor& visitor, Key& full) const {

    if (const auto key = findMatchingKey(field)) {

        full.pushFrom(*key);

        if (visitor.selectIndex(*key) || visitor.createIndex(*key, rule_->size())) {
            if (rule_->expand(field, visitor, full)) {
                return true;
            }
        }

        full.popFrom(*key);
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------
// RULE DATABASE

RuleDatabase::RuleDatabase(const std::size_t line, Predicates& predicates, const eckit::StringDict& types,
                           Children& rules) :
    Rule(line, predicates, types), rules_{std::move(rules)} {}

RuleDatabase::RuleDatabase(eckit::Stream& stream) : Rule() {
    decode(stream);

    size_t numRules;
    stream >> numRules;
    rules_.reserve(numRules);

    for (size_t i = 0; i < numRules; i++) {
        rules_.emplace_back(new RuleIndex(stream));
    }
}

void RuleDatabase::encode(eckit::Stream& out) const {
    Rule::encode(out);
    out << rules_.size();
    for (const auto& rule : rules_) {
        rule->encode(out);
    }
}

void RuleDatabase::updateParent(const Rule* /* parent */) {
    for (auto& rule : rules_) {
        rule->updateParent(this);
    }
}

void RuleDatabase::expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const {

    for (auto& key : findMatchingKeys(request, visitor)) {

        if (visitor.selectDatabase(key, key)) {
            // (important) using the database's schema
            for (const auto& rule : visitor.databaseSchema().matchingRule(key).rules()) {
                rule->expand(request, visitor, key);
            }
            visitor.deselectDatabase();
        }
    }
}

bool RuleDatabase::expand(const Key& field, WriteVisitor& visitor) const {

    if (auto key = findMatchingKey(field)) {

        if (visitor.selectDatabase(*key, *key)) {
            // (important) using the database's schema
            for (const auto& rule : visitor.databaseSchema().matchingRule(*key).rules()) {
                if (rule->expand(field, visitor, *key)) {
                    return true;
                }
            }
        }
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
