/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PMemDB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_PMemDB_H
#define fdb5_PMemDB_H

#include "eckit/memory/ScopedPtr.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/pmem/PMemPool.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class PMemDB : public DB {

public: // methods

    PMemDB(const Key& key);
    PMemDB(const eckit::PathName& directory);

    virtual ~PMemDB();

protected: // methods

    virtual bool open();
    virtual void close();
    virtual void flush();

     virtual eckit::DataHandle *retrieve(const Key &key) const;
     virtual void archive(const Key &key, const void *data, eckit::Length length);
     virtual void axis(const std::string &keyword, eckit::StringSet &s) const;

    // void loadSchema();
     virtual void checkSchema(const Key &key) const;

    virtual bool selectIndex(const Key &key);
    virtual void deselectIndex();

private: // methods

    /// Initialise or open the peristent pool. Worker function for the construtor
    void initialisePool(const eckit::PathName& poolFile);

private: // members

    eckit::ScopedPtr<PMemPool> pool_;

//    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
