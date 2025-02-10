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
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "eckit/filesystem/PathName.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Streamable.h"

#include "fdb5/config/Config.h"
#include "fdb5/rules/Rule.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Key;
class ReadVisitor;
class WriteVisitor;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Schema : public eckit::Streamable {

public:  // methods

    Schema();
    Schema(const eckit::PathName& path);
    Schema(std::istream& stream);
    Schema(eckit::Stream& stream);

    ~Schema() override;

    // expand

    void expand(const Key& field, WriteVisitor& visitor) const;

    void expand(const metkit::mars::MarsRequest& request, ReadVisitor& visitor) const;

    std::vector<Key> expandDatabase(const metkit::mars::MarsRequest& request) const;

    // match

    void matchDatabase(const metkit::mars::MarsRequest& request, std::map<Key, const Rule*>& result,
                       const char* missing) const;

    void matchDatabase(const Key& dbKey, std::map<Key, const Rule*>& result, const char* missing) const;

    std::optional<Key> matchDatabase(const std::string& fingerprint) const;

    /// @throws eckit::SeriousBug if no rule is found
    const RuleDatabase& matchingRule(const Key& dbKey) const;

    /// @throws eckit::SeriousBug if no rule is found
    const RuleDatum& matchingRule(const Key& dbKey, const Key& idxKey) const;

    void load(const eckit::PathName& path, bool replace = false);

    void load(std::istream& s, bool replace = false);

    // accessors

    void dump(std::ostream& s) const;

    bool empty() const;

    const Type& lookupType(const std::string& keyword) const;

    const std::string& path() const;

    const TypesRegistry& registry() const;

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void check();

    void clear();

    void encode(eckit::Stream& stream) const override;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& out, const Schema& schema);

    friend void Config::overrideSchema(const eckit::PathName& schemaPath, Schema* schema);

private:  // members

    TypesRegistry registry_;

    RuleList rules_;

    std::string path_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<Schema> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

/// Schemas are persisted in this registry
///
class SchemaRegistry {
public:

    static SchemaRegistry& instance();

    const Schema& add(const eckit::PathName& path, Schema* schema);
    const Schema& get(const eckit::PathName& path);

private:

    std::mutex m_;
    std::map<eckit::PathName, std::unique_ptr<Schema>> schemas_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
