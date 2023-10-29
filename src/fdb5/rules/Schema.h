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

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/serialisation/Streamable.h"
#include "eckit/serialisation/Reanimator.h"

#include "fdb5/config/Config.h"
#include "fdb5/types/TypesRegistry.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class Key;
class Rule;
class ReadVisitor;
class WriteVisitor;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class Schema : public eckit::Streamable {

public: // methods

    Schema(const eckit::PathName &path);
    Schema(std::istream& s);
    Schema(eckit::Stream& s);

    ~Schema();

    void expand(const Key &field, WriteVisitor &visitor) const;
    void expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const;

    // Each database has its own internal schema. So expand() above results in
    // expandFurther being called on the relevant schema from the DB, to start
    // iterating on that schemas rules.
    void expandSecond(const Key& field, WriteVisitor &visitor, const Key& dbKey) const;
    void expandSecond(const metkit::mars::MarsRequest& request, ReadVisitor &visitor, const Key& dbKey) const;

    bool expandFirstLevel(const Key &dbKey,  InspectionKey &result) const ;
    bool expandFirstLevel(const metkit::mars::MarsRequest& request,  InspectionKey& result) const ;
    void matchFirstLevel(const Key &dbKey,  std::set<InspectionKey> &result, const char* missing) const ;
    void matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<InspectionKey>& result, const char* missing) const ;

    const Rule* ruleFor(const Key &dbKey, const Key& idxKey) const;

    void load(const eckit::PathName &path, bool replace = false);
    void load(std::istream& s, bool replace = false);

    void dump(std::ostream &s) const;

    bool empty() const;

    const Type &lookupType(const std::string &keyword) const;

    const std::string &path() const;

    const TypesRegistry& registry() const;

	const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
	static const eckit::ClassSpec&  classSpec()        { return classSpec_; }

private: // methods

    void clear();
    void check();

    friend std::ostream &operator<<(std::ostream &s, const Schema &x);

    void encode(eckit::Stream& s) const override;

    void print( std::ostream &out ) const;

private: // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<Schema> reanimator_;

    friend void Config::overrideSchema(const eckit::PathName& schemaPath, Schema* schema);

    TypesRegistry registry_;
    std::vector<Rule *>  rules_;
    std::string path_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
