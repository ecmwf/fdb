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

namespace fdb5 {

class DB;
class FDBToolRequest;
class Field;
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

    virtual void visitDatabase(const DB& db);
    virtual void visitIndex(const Index& index);
    virtual void databaseComplete(const DB& db);
    virtual void visitDatum(const Field& field, const std::string& keyFingerprint);

private: // methods

    virtual void visitDatum(const Field& field, const Key& key) = 0;

protected:  // members

    // n.b. non-owning
    const DB* currentDatabase_;
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
