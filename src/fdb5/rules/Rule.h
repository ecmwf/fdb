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

//----------------------------------------------------------------------------------------------------------------------

class Rule {
    friend class Schema;

public:  // methods
    virtual ~Rule() = default;

    Rule(Rule&&)            = default;
    Rule& operator=(Rule&&) = default;

    Rule(const Rule&)            = delete;
    Rule& operator=(const Rule&) = delete;

    virtual const char* type() const = 0;

    virtual void updateParent(const Rule* parent);

    bool match(const Key& key) const;

    bool tryFill(Key& key, const eckit::StringList& values) const;

    void fill(Key& key, const eckit::StringList& values) const;

    void check(const Key& key) const;

    void dump(std::ostream& out) const;

    const Rule& topRule() const;

    /// @todo wrong constness
    const std::shared_ptr<TypesRegistry> registry() const;

protected:  // methods
    Rule(std::size_t line, std::vector<Predicate>& predicates, const eckit::StringDict& types);

    std::unique_ptr<Key> findMatchingKey(const Key& field) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

private:  // methods
    virtual void dumpRules(std::ostream& out) const = 0;

    std::unique_ptr<Key> findMatchingKey(const eckit::StringList& values) const;

    std::unique_ptr<Key> findMatchingKey(const Key& field, const char* missing) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, const char* missing) const;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Rule& x);

private:  // members
    const Rule* parent_ {nullptr};

    std::size_t line_ {0};

    std::vector<Predicate> predicates_;

    std::shared_ptr<TypesRegistry> registry_;
};

//----------------------------------------------------------------------------------------------------------------------
// RULE DATUM

class RuleDatum : public Rule {
public:  // methods
    RuleDatum(std::size_t line, std::vector<Predicate>& predicates, const eckit::StringDict& types);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    bool expand(const Key& field, WriteVisitor& visitor, Key& full) const;

    const char* type() const override { return "RuleDatum"; }

private:  // methods
    void dumpRules(std::ostream& /* out */) const override { }
};

//----------------------------------------------------------------------------------------------------------------------
// RULE INDEX

class RuleIndex : public Rule {
public:  // methods
    RuleIndex(std::size_t line, std::vector<Predicate>& predicates, const eckit::StringDict& types,
              std::vector<RuleDatum>& rules);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    bool expand(const Key& field, WriteVisitor& visitor, Key& full) const;

    void updateParent(const Rule* parent) override;

    const std::vector<RuleDatum>& rules() const { return rules_; }

    const char* type() const override { return "RuleIndex"; }

private:  // methods
    void dumpRules(std::ostream& out) const override {
        for (const auto& rule : rules_) { rule.dump(out); }
    }

private:  // methods
    std::vector<RuleDatum> rules_;
};

//----------------------------------------------------------------------------------------------------------------------
// RULE DATABASE

class RuleDatabase : public Rule {
public:  // methods
    RuleDatabase(std::size_t line, std::vector<Predicate>& predicates, const eckit::StringDict& types,
                 std::vector<RuleIndex>& rules);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    bool expand(const Key& field, WriteVisitor& visitor) const;

    void updateParent(const Rule* /* parent */) override;

    const std::vector<RuleIndex>& rules() const { return rules_; }

    const char* type() const override { return "RuleDatabase"; }

private:  // methods
    void dumpRules(std::ostream& out) const override {
        for (const auto& rule : rules_) { rule.dump(out); }
    }

private:  // methods
    std::vector<RuleIndex> rules_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
