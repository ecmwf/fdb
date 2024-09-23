/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Schema.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Schema_H
#define fdb5_Schema_H

#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "eckit/filesystem/PathName.h"
#include "eckit/memory/NonCopyable.h"

#include "fdb5/rules/Rule.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
class Database;
class ReadVisitor;
class WriteVisitor;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Schema : private eckit::NonCopyable {

public:  // methods
    Schema();
    Schema(const eckit::PathName& path);
    Schema(std::istream& s);

    ~Schema();

    // EXPAND

    void expand(const Key& field, WriteVisitor& visitor) const;

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    std::vector<Key> expandDatabase(const metkit::mars::MarsRequest& request) const;

    // MATCH

    void matchDatabase(const metkit::mars::MarsRequest& request, std::set<Key>& result, const char* missing) const;

    void matchDatabase(const Key& dbKey, std::set<Key>& result, const char* missing) const;

    std::unique_ptr<Key> matchDatabase(const std::string& fingerprint) const;

    const RuleDatabase& matchingRule(const Key& dbKey) const;

    /// @todo return RuleDatum
    const Rule* matchingRule(const Key& dbKey, const Key& idxKey) const;

    void load(const eckit::PathName& path, bool replace = false);

    void load(std::istream& s, bool replace = false);

    // ACCESSORS

    void dump(std::ostream& s) const;

    bool empty() const;

    const Type& lookupType(const std::string& keyword) const;

    const std::string& path() const;

    std::shared_ptr<const TypesRegistry> registry() const;

private: // methods

    void clear();

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const Schema& x);

private:  // members
    std::shared_ptr<TypesRegistry> registry_;

    std::vector<RuleDatabase> rules_;

    std::string path_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Schemas are persisted in this registry
///
class SchemaRegistry {
public:
    static SchemaRegistry& instance();
    const Schema& get(const eckit::PathName& path);

private:
    std::mutex m_;
    std::map<eckit::PathName, std::unique_ptr<Schema>> schemas_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
