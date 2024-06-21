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
#include <vector>
#include <memory>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/memory/NonCopyable.h"

#include "fdb5/types/TypesRegistry.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class Key;
class Rule;
class ReadVisitor;
class WriteVisitor;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class Schema : private eckit::NonCopyable {

public: // methods

    Schema();
    Schema(const eckit::PathName &path);
    Schema(std::istream& s);

    ~Schema();

    void expand(const Key& field, WriteVisitor &visitor) const;
    void expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const;

    // Each database has its own internal schema. So expand() above results in
    // expandFurther being called on the relevant schema from the DB, to start
    // iterating on that schemas rules.
    void expandSecond(const Key& field, WriteVisitor &visitor, const Key& dbKey) const;
    void expandSecond(const metkit::mars::MarsRequest& request, ReadVisitor &visitor, const Key& dbKey) const;

    // bool expandFirstLevel(const Key& dbKey,  Key& result) const ;
    bool expandFirstLevel(const metkit::mars::MarsRequest& request,  TypedKey& result) const ;
    void matchFirstLevel(const Key& dbKey,  std::set<Key> &result, const char* missing) const ;
    void matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const ;

    const Rule* ruleFor(const Key& dbKey, const Key& idxKey) const;

    void load(const eckit::PathName &path, bool replace = false);
    void load(std::istream& s, bool replace = false);

    void dump(std::ostream &s) const;

    bool empty() const;

    const Type &lookupType(const std::string &keyword) const;

    const std::string &path() const;

    const std::shared_ptr<TypesRegistry> registry() const;


private: // methods

    void clear();
    void check();

    friend std::ostream &operator<<(std::ostream &s, const Schema &x);

    void print( std::ostream &out ) const;

private: // members

    std::shared_ptr<TypesRegistry> registry_;
    std::vector<Rule *>  rules_;
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
