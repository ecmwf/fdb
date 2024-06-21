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
#include <memory>

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
class Key;
class TypedKey;

//----------------------------------------------------------------------------------------------------------------------

class Rule : public eckit::NonCopyable {

public: // methods

    /// Takes ownership of vectors
    Rule(const Schema &schema,
         size_t line,
         std::vector<Predicate *> &predicates,
         std::vector<Rule *> &rules,
         const std::map<std::string, std::string> &types
        );

    ~Rule();

    bool match(const Key& key) const;

    eckit::StringList keys(size_t level) const;

    void dump(std::ostream &s, size_t depth = 0) const;

    void expand(const metkit::mars::MarsRequest &request,
                ReadVisitor &Visitor,
                size_t depth,
                std::vector<fdb5::TypedKey> &keys,
                TypedKey& fullComputedKey) const;

    void expand(const Key& initialFieldKey,
                WriteVisitor &Visitor,
                size_t depth,
                std::vector<fdb5::TypedKey> &keys,
                TypedKey& fullComputedKey) const;

    const Rule* ruleFor(const std::vector<fdb5::Key> &keys, size_t depth) const;
    void fill(TypedKey& key, const eckit::StringList& values) const;


    size_t depth() const;
    void updateParent(const Rule *parent);

    const Rule &topRule() const;

    const Schema &schema() const;
    const std::shared_ptr<TypesRegistry> registry() const;

    void check(const Key& key) const;

private: // methods

    void expand(const metkit::mars::MarsRequest &request,
                std::vector<Predicate *>::const_iterator cur,
                size_t depth,
                std::vector<TypedKey> &keys,
                TypedKey& fullComputedKey,
                ReadVisitor &Visitor) const;

    void expand(const Key& initialFieldKey,
                std::vector<Predicate *>::const_iterator cur,
                size_t depth,
                std::vector<TypedKey> &keys,
                TypedKey& fullComputedKey,
                WriteVisitor &Visitor) const;

    // void expandFirstLevel(const Key& dbKey, std::vector<Predicate *>::const_iterator cur, Key& result, bool& done) const;
    // void expandFirstLevel(const Key& dbKey,  Key& result, bool& done) const ;
    void expandFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate *>::const_iterator cur, TypedKey& result, bool& done) const;
    void expandFirstLevel(const metkit::mars::MarsRequest& request,  TypedKey& result, bool& done) const;

    void matchFirstLevel(const Key& dbKey, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const;
    void matchFirstLevel(const Key& dbKey, std::set<Key>& result, const char* missing) const ;
    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const;
    void matchFirstLevel(const metkit::mars::MarsRequest& request, std::set<Key>& result, const char* missing) const ;


    void keys(size_t level, size_t depth, eckit::StringList &result, eckit::StringSet &seen) const;

    friend std::ostream &operator<<(std::ostream &s, const Rule &x);

    void print( std::ostream &out ) const;


private: // members

    const Schema& schema_;
    const Rule* parent_;

    std::vector<Predicate *> predicates_;
    std::vector<Rule *>      rules_;

    std::shared_ptr<TypesRegistry> registry_;

    friend class Schema;
    size_t line_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
