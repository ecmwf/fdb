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
#include <optional>
#include <vector>

#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Streamable.h"
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

class Rule : public eckit::Streamable {
    friend class Schema;

public:  // types

    using Predicates = std::vector<std::unique_ptr<Predicate>>;

public:  // methods

    Rule(std::size_t line, Predicates& predicates, const eckit::StringDict& types);

    virtual const char* type() const = 0;

    virtual void updateParent(const Rule* parent);

    /// @todo this different from the other findMatchingKey in that it throws and fixes quantile values
    /// can we merge them ?
    Key makeKey(const std::string& keyFingerprint) const;

    bool match(const Key& key) const;

    void check(const Key& key) const;

    void dump(std::ostream& out) const;

    const Rule& parent() const;
    const Rule& topRule() const;
    bool isTopRule() const;

    const TypesRegistry& registry() const;

    void encode(eckit::Stream& out) const override;

protected:  // methods

    Rule() = default;

    void decode(eckit::Stream& stream);

    std::optional<Key> findMatchingKey(const Key& field) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

private:  // methods

    virtual void dumpChildren(std::ostream& out) const = 0;

    std::optional<Key> findMatchingKey(const eckit::StringList& values) const;

    std::optional<Key> findMatchingKey(const Key& field, const char* missing) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, const char* missing) const;

    bool tryFill(Key& key, const eckit::StringList& values) const;

    void fill(Key& key, const eckit::StringList& values) const;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& out, const Rule& rule);

protected:  // members

    const Rule* parent_{nullptr};

    std::size_t line_{0};

    Predicates predicates_;

    TypesRegistry registry_;

private:  // members

    // streamable
    static eckit::ClassSpec classSpec_;
};

//----------------------------------------------------------------------------------------------------------------------
// RULE DATUM

class RuleDatum : public Rule {
public:  // methods

    using Rule::Rule;

    explicit RuleDatum(eckit::Stream& stream);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    bool expand(const Key& field, WriteVisitor& visitor, Key& full) const;

    const char* type() const override { return "RuleDatum"; }

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

    void encode(eckit::Stream& out) const override;

private:  // methods

    void dumpChildren(std::ostream& /* out */) const override {}

private:  // members

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RuleDatum> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------
// RULE INDEX

class RuleIndex : public Rule {
public:  // types

    using Children = std::vector<std::unique_ptr<RuleDatum>>;

public:  // methods

    RuleIndex(std::size_t line, Predicates& predicates, const eckit::StringDict& types, Children& rules);

    explicit RuleIndex(eckit::Stream& stream);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    bool expand(const Key& field, WriteVisitor& visitor, Key& full) const;

    void updateParent(const Rule* parent) override;

    const Children& rules() const { return rules_; }

    const char* type() const override { return "RuleIndex"; }

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

    void encode(eckit::Stream& out) const override;

private:  // methods

    void dumpChildren(std::ostream& out) const override {
        for (const auto& rule : rules_) {
            rule->dump(out);
        }
    }

private:  // members

    Children rules_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RuleIndex> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------
// RULE DATABASE

class RuleDatabase : public Rule {
public:  // types

    using Children = std::vector<std::unique_ptr<RuleIndex>>;

public:  // methods

    RuleDatabase(std::size_t line, Predicates& predicates, const eckit::StringDict& types, Children& rules);

    explicit RuleDatabase(eckit::Stream& stream);

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    bool expand(const Key& field, WriteVisitor& visitor) const;

    void updateParent(const Rule* parent) override;

    const Children& rules() const { return rules_; }

    const char* type() const override { return "RuleDatabase"; }

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

    void encode(eckit::Stream& out) const override;

private:  // methods

    void dumpChildren(std::ostream& out) const override {
        for (const auto& rule : rules_) {
            rule->dump(out);
        }
    }

private:  // members

    Children rules_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RuleDatabase> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

using RuleList = std::vector<std::unique_ptr<RuleDatabase>>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
