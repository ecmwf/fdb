/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_api_FDB_H
#define fdb5_api_FDB_H

#include <iosfwd>
#include <memory>

#include "eckit/distributed/Transport.h"

#include "fdb5/api/FDBStats.h"
#include "fdb5/api/helpers/ArchiveCallback.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"

namespace eckit {
namespace message {
class Message;
}
class DataHandle;
}  // namespace eckit

namespace metkit {
class MarsRequest;
}

namespace fdb5 {

class FDBBase;
class FDBToolRequest;
class Key;

//----------------------------------------------------------------------------------------------------------------------

/// A handle to a general FDB

class FDB {

public:  // methods

    FDB(const Config& config = Config().expandConfig());
    ~FDB();

    FDB(const FDB&)            = delete;
    FDB& operator=(const FDB&) = delete;

    FDB(FDB&&)            = default;
    FDB& operator=(FDB&&) = default;

    // -------------- Primary API functions ----------------------------

    void archive(eckit::message::Message msg);
    void archive(eckit::DataHandle& handle);
    void archive(const void* data, size_t length);
    // warning: not high-perf API - makes sure that all the requested fields are archived and there are no data
    // exceeding the request
    void archive(const metkit::mars::MarsRequest& request, eckit::DataHandle& handle);

    // disclaimer: this is a low-level API. The provided key and the corresponding data are not checked for consistency
    // Optional callback function is called upon receiving field location from the store.
    void archive(const Key& key, const void* data, size_t length);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    eckit::DataHandle* read(const eckit::URI& uri);

    eckit::DataHandle* read(const std::vector<eckit::URI>& uris, bool sorted = false);

    eckit::DataHandle* read(ListIterator& it, bool sorted = false);

    eckit::DataHandle* retrieve(const metkit::mars::MarsRequest& request);

    ListIterator inspect(const metkit::mars::MarsRequest& request);

    ListIterator list(const FDBToolRequest& request, bool deduplicate = false);

    DumpIterator dump(const FDBToolRequest& request, bool simple = false);

    /// TODO: Is this function superfluous given the control() function?
    StatusIterator status(const FDBToolRequest& request);

    WipeIterator wipe(const FDBToolRequest& request, bool doit = false, bool porcelain = false,
                      bool unsafeWipeAll = false);

    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest);

    PurgeIterator purge(const FDBToolRequest& request, bool doit = false, bool porcelain = false);

    StatsIterator stats(const FDBToolRequest& request);

    ControlIterator control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers);

    IndexAxis axes(const FDBToolRequest& request, int level = 3);

    bool enabled(const ControlIdentifier& controlIdentifier) const;

    bool dirty() const;

    void registerCallback(ArchiveCallback callback);

    // -------------- API management ----------------------------

    /// ID used for hashing in the Rendezvous hash. Should be unique.
    const std::string id() const;

    FDBStats stats() const;
    FDBStats internalStats() const;

    const std::string& name() const;
    const Config& config() const;

    void disable();
    bool disabled() const;

private:  // methods

    void print(std::ostream&) const;

    friend std::ostream& operator<<(std::ostream& s, const FDB& f) {
        f.print(s);
        return s;
    }

    bool sorted(const metkit::mars::MarsRequest& request);

private:  // members

    std::unique_ptr<FDBBase> internal_;

    bool dirty_;
    bool reportStats_;

    FDBStats stats_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_api_FDB_H
