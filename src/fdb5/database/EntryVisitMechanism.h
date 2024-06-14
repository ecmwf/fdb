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

#include "eckit/memory/NonCopyable.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/DatabaseNotFoundException.h"

namespace fdb5 {

class Catalogue;
class Store;
class FDBToolRequest;
class Index;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitor : public eckit::NonCopyable {

public:  // methods

    EntryVisitor();
    virtual ~EntryVisitor();

    // defaults
    virtual bool visitIndexes() { return true; }
    virtual bool visitEntries() { return true; }

    virtual bool visitDatabase(const Catalogue& catalogue, const Store& store);    // return true if Catalogue should be explored
    virtual bool visitIndex(const Index& index); // return true if index should be explored
    virtual void catalogueComplete(const Catalogue& catalogue);
    virtual void visitDatum(const Field& field, const std::string& keyFingerprint);

    virtual void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) {}

    time_t indexTimestamp() const;

private: // methods

    virtual void visitDatum(const Field& field, const Key& key) = 0;

protected:  // members

    // n.b. non-owning
    const Catalogue* currentCatalogue_;
    const Store* currentStore_;
    const Index* currentIndex_;
};

//----------------------------------------------------------------------------------------------------------------------

class EntryVisitMechanism : public eckit::NonCopyable {

public:  // methods

    EntryVisitMechanism(const Config& config);

    void visit(const FDBToolRequest& request, EntryVisitor& visitor);

private:  // members

    const Config& dbConfig_;

    // Fail on error
    bool fail_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
