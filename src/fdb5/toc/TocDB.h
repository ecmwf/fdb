/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocDB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_TocDB_H
#define fdb5_TocDB_H

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDB : public DB, public TocHandler {

public: // methods

    TocDB(const Key& key);
    TocDB(const eckit::PathName& directory);

    virtual ~TocDB();

    static const char* dbTypeName() { return TocEngine::typeName(); }

protected: // methods

    virtual std::string dbType() const;

    virtual bool open();
    virtual void close();
    virtual void flush();
    virtual bool exists() const;
    virtual void visitEntries(EntryVisitor& visitor);
    virtual void visit(DBVisitor& visitor);
    virtual void dump(std::ostream& out, bool simple=false);
    virtual eckit::PathName basePath() const;
    virtual const Schema& schema() const;

    virtual eckit::DataHandle *retrieve(const Key &key) const;
    virtual void archive(const Key &key, const void *data, eckit::Length length);
    virtual void axis(const std::string &keyword, eckit::StringSet &s) const;

    void loadSchema();
    void checkSchema(const Key &key) const;

    virtual DbStats statistics() const;

    std::vector<Index> indexes() const;

private: // members

    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
