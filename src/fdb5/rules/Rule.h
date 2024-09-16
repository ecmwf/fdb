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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "eckit/memory/NonCopyable.h"
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
class KeyChain;

//----------------------------------------------------------------------------------------------------------------------

class Rule : public eckit::NonCopyable {

public: // methods
    Rule(const Schema& schema, std::size_t line, std::vector<Predicate*>&& predicates, std::vector<Rule*>&& rules,
         const std::map<std::string, std::string>& types);

    ~Rule();

    bool match(const Key &key) const;

    eckit::StringList keys(size_t level) const;

    void dump(std::ostream &s, size_t depth = 0) const;

    void expandDatabase(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const;
    void expandIndex(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const;
    void expandDatum(const Key& field, WriteVisitor& visitor, KeyChain& keys, Key& full) const;

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    std::vector<const Rule*> subRulesView() const { return {rules_.begin(), rules_.end()}; }

    const Rule* ruleFor(const KeyChain& keys, size_t depth) const;

    bool tryFill(Key& key, const eckit::StringList& values) const;

    void fill(Key& key, const eckit::StringList& values) const;

    size_t depth() const;

    void updateParent(const Rule *parent);

    const Rule &topRule() const;

    const Schema &schema() const;

    /// @todo wrong constness
    const std::shared_ptr<TypesRegistry> registry() const;

    void check(const Key& key) const;

    const std::vector<Rule*>&      subRules() const;
    const std::vector<Predicate*>& predicates() const;

private:  // methods
    std::unique_ptr<Key> findMatchingKey(const eckit::StringList& values) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request) const;

    std::vector<Key> findMatchingKeys(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    void expandDatum(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;
    void expandIndex(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, Key& full) const;

    void expand(const Key& field, std::vector<Predicate*>::const_iterator cur, std::size_t depth, KeyChain& keys,
                Key& full, WriteVisitor& Visitor) const;

    // expand key

    void expandFirstLevel(const Key& dbKey, std::vector<Predicate*>::const_iterator cur, Key& result, bool& found) const;
    void expandFirstLevel(const Key& dbKey, Key& result, bool& found) const;

    // match key

    void matchFirstLevel(const Key& dbKey, std::vector<Predicate*>::const_iterator cur, Key& tmp, std::set<Key>& result,
                         const char* missing) const;
    void matchFirstLevel(const Key& dbKey, std::set<Key>& result, const char* missing) const;

    // match request

    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate*>::const_iterator cur,
                         Key& tmp, std::set<Key>& result, const char* missing) const;
    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::set<Key>& result, const char* missing) const;

    void keys(size_t level, size_t depth, eckit::StringList& result, eckit::StringSet& seen) const;

    friend std::ostream& operator<<(std::ostream& s, const Rule& x);

    void print(std::ostream& out) const;

private:  // members
    friend class Schema;

    const Schema& schema_;

    std::size_t line_ {0};

    std::vector<Predicate*> predicates_;

    std::vector<Rule*> rules_;

    std::shared_ptr<TypesRegistry> registry_;

    const Rule* parent_ {nullptr};
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
