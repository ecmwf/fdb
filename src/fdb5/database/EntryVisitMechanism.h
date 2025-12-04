/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_EntryVisitMechanism_H
#define fdb5_EntryVisitMechanism_H

#include "fdb5/config/Config.h"
#include "fdb5/database/DatabaseNotFoundException.h"
#include "fdb5/database/Field.h"

namespace eckit {
class URI;
}

namespace fdb5 {

class Catalogue;
class Store;
class FDBToolRequest;
class Index;
class Rule;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitor {

public:  // methods

    EntryVisitor();

    EntryVisitor(const EntryVisitor&)            = delete;
    EntryVisitor& operator=(const EntryVisitor&) = delete;
    EntryVisitor(EntryVisitor&&)                 = delete;
    EntryVisitor& operator=(EntryVisitor&&)      = delete;

    virtual ~EntryVisitor();

    // defaults
    virtual bool visitIndexes() { return true; }
    virtual bool visitEntries() { return true; }

    virtual bool preVisitDatabase(const eckit::URI& uri, const Schema& schema);
    virtual bool visitDatabase(const Catalogue& catalogue);  // return true if Catalogue should be explored
    virtual bool visitIndex(const Index& index);             // return true if index should be explored
    virtual void catalogueComplete(const Catalogue& catalogue);
    virtual void visitDatum(const Field& field, const std::string& keyFingerprint);

    virtual void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) {}

    time_t indexTimestamp() const;

protected:

    Store& store() const;
    virtual void visitDatum(const Field& field, const Key& datumKey) = 0;

protected:  // members

    /// Non-owning
    const Catalogue* currentCatalogue_{nullptr};
    /// Owned store
    mutable Store* currentStore_{nullptr};
    /// Non-owning
    const Index* currentIndex_{nullptr};
    /// Non-owning
    const Rule* rule_{nullptr};
};

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitMechanism {

public:  // methods

    EntryVisitMechanism(const Config& config);

    EntryVisitMechanism(const EntryVisitMechanism&)            = delete;
    EntryVisitMechanism& operator=(const EntryVisitMechanism&) = delete;
    EntryVisitMechanism(EntryVisitMechanism&&)                 = delete;
    EntryVisitMechanism& operator=(EntryVisitMechanism&&)      = delete;

    void visit(const FDBToolRequest& request, EntryVisitor& visitor);

private:  // members

    const Config& dbConfig_;

    // Fail on error
    bool fail_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
