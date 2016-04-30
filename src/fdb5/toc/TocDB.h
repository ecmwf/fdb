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

#include "fdb5/DB.h"
#include "fdb5/Index.h"
#include "fdb5/rule/Schema.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDB : public DB, public TocHandler {

public: // methods

    TocDB(const Key& dbKey);
    TocDB(const eckit::PathName& directory);

    virtual ~TocDB();

protected: // methods

    virtual bool open();
    virtual void close();
    virtual void flush();

    virtual eckit::DataHandle *retrieve(const Key &key) const;
    virtual void archive(const Key &key, const void *data, eckit::Length length);
    virtual void axis(const std::string &keyword, eckit::StringSet &s) const;

    void loadSchema();
    void checkSchema(const Key &key) const;

private: // members

    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
