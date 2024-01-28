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

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"
#include "fdb5/types/TypesRegistry.h"

namespace metkit {
namespace mars {
    class MarsRequest;
}
}

namespace fdb5 {

class Schema;
class Predicate;
class ReadVisitor;
class WriteVisitor;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class Rule : public eckit::NonCopyable {

public: // methods

    /// Takes ownership of vectors
    Rule(const Schema &schema,
         size_t line,
         std::vector<Predicate *>&& predicates,
         std::vector<Rule *>&& rules,
         const std::map<std::string, std::string> &types,
         long level);

    virtual ~Rule();

    static Rule* makeRule(int level,
                          const Schema& schema,
                          size_t line,
                          std::vector<Predicate *>&& predicates,
                          std::vector<Rule *>&& rules,
                          const std::map<std::string, std::string>& types);

    void expand(const Key &field,
                WriteVisitor& visitor,
                std::vector<fdb5::Key> &keys,
                Key &full) const;

    void expand(const metkit::mars::MarsRequest& request,
                ReadVisitor& visitor,
                std::vector<Key>& keys,
                Key& full) const;

    bool match(const Key &key) const;

    eckit::StringList keys(size_t level) const;

    void dump(std::ostream &s, size_t depth = 0) const;

    const Rule* ruleFor(const std::vector<fdb5::Key> &keys, size_t depth) const;
    bool tryFill(Key& key, const eckit::StringList& values) const;
    void fill(Key& key, const eckit::StringList& values) const;
    const std::vector<Rule*>& subRules() const;

    size_t depth() const;
    void updateParent(const Rule *parent);

    const Rule &topRule() const;

    const Schema &schema() const;
    const TypesRegistry &registry() const;

    void check(const Key& key) const;

    const std::vector<Predicate*>& predicates() const;

protected: // methods

    virtual void walkNextLevel(const Key& field,
                               WriteVisitor& visitor,
                               std::vector<fdb5::Key>& keys,
                               Key& full) const = 0;

    virtual void walkNextLevel(const metkit::mars::MarsRequest& request,
                               ReadVisitor& visitor,
                               std::vector<fdb5::Key>& keys,
                               Key& full) const = 0;

    friend std::ostream &operator<<(std::ostream &s, const Rule &x);

    void print( std::ostream &out ) const;


protected: // members

    const Schema& schema_;
    const Rule* parent_;
    long level_;

    std::vector<Predicate *> predicates_;
    std::vector<Rule *>      rules_;

    TypesRegistry registry_;

    friend class Schema;
    size_t line_;

};

//----------------------------------------------------------------------------------------------------------------------

class RuleThird : public Rule {

public: // methods

    using TypeNext = RuleThird; // Avoid issue with infinite recursion in Schema parser...

    RuleThird(const Schema& schema,
              size_t line,
              std::vector<Predicate *>&& predicates,
              std::vector<Rule *>&& rules,
              const std::map<std::string, std::string>& types);

protected: // implementations for use by RuleCommon

    void walkNextLevel(const Key& field,
                       WriteVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;

    void walkNextLevel(const metkit::mars::MarsRequest& request,
                       ReadVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;
};

//----------------------------------------------------------------------------------------------------------------------

class RuleSecond : public Rule {

public: // methods

    using TypeNext = RuleThird;

    RuleSecond(const Schema& schema,
               size_t line,
               std::vector<Predicate *>&& predicates,
               std::vector<Rule *>&& rules,
               const std::map<std::string, std::string>& types);

protected: // implementations for use by RuleCommon

    void walkNextLevel(const Key& field,
                       WriteVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;

    void walkNextLevel(const metkit::mars::MarsRequest& request,
                       ReadVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;

public: // methods
};

//----------------------------------------------------------------------------------------------------------------------

class RuleFirst : public Rule {

public: // methods

    using TypeNext = RuleSecond;

    RuleFirst(const Schema& schema,
              size_t line,
              std::vector<Predicate *>&& predicates,
              std::vector<Rule *>&& rules,
              const std::map<std::string, std::string>& types);

protected: // implementations for use by RuleCommon

    void walkNextLevel(const Key& field,
                       WriteVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;

    void walkNextLevel(const metkit::mars::MarsRequest& request,
                       ReadVisitor& visitor,
                       std::vector<fdb5::Key>& keys,
                       Key& full) const override;

public: // methods

    using Rule::expand;

    // Level-specific functions

    void expandFirstLevel(const Key& dbKey, std::vector<Predicate *>::const_iterator cur, Key& result, bool& done) const;
    void expandFirstLevel(const Key& dbKey,  Key& result, bool& done) const ;
    void expandFirstLevel(const metkit::mars::MarsRequest& request,
                          std::vector<Predicate *>::const_iterator cur,
                          std::vector<Key>& result,
                          Key& working,
                          bool& found) const;

    void expandFirstLevel(const metkit::mars::MarsRequest& request,  std::vector<Key>& result, bool& found) const;

    void matchFirstLevel(const Key& dbKey, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const;
    void matchFirstLevel(const Key& dbKey, std::set<Key>& result, const char* missing) const ;
    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const;
    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::set<Key>& result, const char* missing) const ;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
