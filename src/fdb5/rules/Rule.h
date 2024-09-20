/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Rule.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Rule_H
#define fdb5_Rule_H

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <vector>

#include "eckit/types/Types.h"

#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/TypesRegistry.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Schema;
class Predicate;
class ReadVisitor;
class WriteVisitor;
class Key;

/// @todo remove this
class KeyChain;

//----------------------------------------------------------------------------------------------------------------------

class Rule {
    friend class Schema;

public:  // types
    using PredList = typename SchemaParser::PredList;
    using RuleList = typename SchemaParser::RuleList;
    using TypeList = typename SchemaParser::TypeList;

public: // methods
    Rule(std::size_t line, PredList&& predicates, RuleList&& rules, const TypeList& types);

    virtual ~Rule() = default;

    Rule(Rule&&)            = default;
    Rule& operator=(Rule&&) = default;

    Rule(const Rule&)            = delete;
    Rule& operator=(const Rule&) = delete;

    // MATCH

    bool match(const Key &key) const;

    bool tryFill(Key& key, const eckit::StringList& values) const;

    void fill(Key& key, const eckit::StringList& values) const;

    void check(const Key& key) const;

    void dump(std::ostream &s, size_t depth = 0) const;

    // EXPAND

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    bool expand(const Key& field, WriteVisitor& visitor) const;

    // ACCESS

    const RuleList& subRules() const { return rules_; }

    const PredList& predicates() const { return predicates_; }

    /// @todo remove this
    const Rule* ruleFor(const KeyChain& keys, size_t depth) const;

    std::size_t depth() const;

    void updateParent(const Rule *parent);

    const Rule &topRule() const;

    eckit::StringList keys(size_t level) const;

    /// @todo wrong constness
    const std::shared_ptr<TypesRegistry> registry() const;

private:  // methods
    std::unique_ptr<Key> findMatchingKey(const eckit::StringList& values) const;

    std::unique_ptr<Key> findMatchingKey(const Key& field) const;

    std::unique_ptr<Key> findMatchingKey(const Key& field, const char* missing) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, const char* missing) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    // EXPAND: READ PATH

    void expandDatum(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    void expandIndex(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    // EXPAND: WRITE PATH

    bool expandDatum(const Key& field, WriteVisitor& visitor, Key& full) const;

    bool expandIndex(const Key& field, WriteVisitor& visitor, Key& full) const;

    void keys(size_t level, size_t depth, eckit::StringList& result, eckit::StringSet& seen) const;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Rule& x);

private:  // members
    std::size_t line_ {0};

    PredList predicates_;

    RuleList rules_;

    std::shared_ptr<TypesRegistry> registry_;

    const Rule* parent_ {nullptr};
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
