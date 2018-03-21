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
/// @date   March 2018

#ifndef fdb5_dist_DistDB_H
#define fdb5_dist_DistDB_H

#include "fdb5/database/DB.h"
#include "fdb5/dist/DistEngine.h"
#include "fdb5/dist/DistManager.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DistDB : public DB {

public: // methods

    DistDB(const Key& key, const eckit::Configuration& config);
    DistDB(const eckit::PathName& directory, const eckit::Configuration& config);

    virtual ~DistDB();

    static const char* dbTypeName() { return DistEngine::typeName(); }

protected: // methods

    virtual std::string dbType() const;

    virtual bool open();
    virtual void close();
    virtual void flush();
    virtual bool exists() const;
    virtual void visitEntries(EntryVisitor& visitor, bool sorted=false);
    virtual void visit(DBVisitor& visitor);
    virtual void dump(std::ostream& out, bool simple=false);
    virtual std::string owner() const;
    virtual eckit::PathName basePath() const;
    virtual const Schema& schema() const;

    virtual eckit::DataHandle *retrieve(const Key &key) const;
    virtual void archive(const Key &key, const void *data, eckit::Length length);
    virtual void axis(const std::string &keyword, eckit::StringSet &s) const;

    void loadSchema();
    void checkSchema(const Key &key) const;

    virtual DbStats statistics() const;

    virtual std::vector<Index> indexes(bool sorted=false) const;

protected: // members

    DistManager manager_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_dist_DistDB_H
