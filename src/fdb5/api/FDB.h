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
/// @date   Mar 2018

#ifndef fdb5_api_FDB_H
#define fdb5_api_FDB_H

#include <memory>
#include <iosfwd>

#include "fdb5/config/Config.h"
#include "fdb5/api/FDBStats.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/helpers/WhereIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"

namespace metkit { class MarsRequest; }

namespace fdb5 {

class FDBBase;
class FDBToolRequest;
class Key;

//----------------------------------------------------------------------------------------------------------------------

/// A handle to a general FDB

class FDB {

public: // methods

    FDB(const Config& config = Config());
    ~FDB();

    FDB(const FDB&) = delete;
    FDB& operator=(const FDB&) = delete;

    FDB(FDB&&) = default;
    FDB& operator=(FDB&&) = default;

    // -------------- Primary API functions ----------------------------

    void archive(const Key& key, const void* data, size_t length);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    eckit::DataHandle* retrieve(const metkit::MarsRequest& request);

    ListIterator list(const FDBToolRequest& request);

    DumpIterator dump(const FDBToolRequest& request, bool simple=false);

    WhereIterator where(const FDBToolRequest& request);

    WipeIterator wipe(const FDBToolRequest& request, bool doit=false, bool porcelain=false);

    PurgeIterator purge(const FDBToolRequest& request, bool doit=false, bool porcelain=false);

    StatsIterator stats(const FDBToolRequest& request);

    bool dirty() const;

    // -------------- API management ----------------------------

    /// ID used for hashing in the Rendezvous hash. Should be unique.
    const std::string id() const;

    FDBStats stats() const;
    FDBStats internalStats() const;

    const std::string& name() const;
    const Config& config() const;

    bool writable() const;
    bool visitable() const;
    bool disabled() const;

    void disable();

private: // methods

    void print(std::ostream&) const;

    friend std::ostream& operator<<(std::ostream& s, const FDB& f) {
        f.print(s);
        return s;
    }

private: // members

    std::unique_ptr<FDBBase> internal_;

    bool dirty_;
    bool reportStats_;

    FDBStats stats_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_api_FDB_H
