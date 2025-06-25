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
#include <string>

#include "eckit/distributed/Transport.h"

#include "fdb5/api/FDBStats.h"
#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/helpers/Callback.h"
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

/// A handle to a general FDB.
///
/// FDB and its methods are threadsafe. However the caller needs to be aware that flush acts on all archive calls,
/// including arcived messages from other threads. I.e. a call to flush will persist all archived messages regardles
/// from which thread the message has been archived. In case the caller wants a finer control it is advised to
/// instanciate one FDB object per thread to ensure only messages are flushed that have been archived on the same FDB
/// object.
class FDB {

public:  // methods

    FDB(const Config& config = Config().expandConfig());
    ~FDB();

    FDB(const FDB&)            = delete;
    FDB& operator=(const FDB&) = delete;

    FDB(FDB&&);
    FDB& operator=(FDB&&);

    // -------------- Primary API functions ----------------------------------------------------------------------------

    /// Archive a eckit::message::Message.
    ///
    /// Due to the message being self describing no key needs to be supplied.
    /// Any callback set with registerArchiveCallback will be invoked.
    /// @param handle eckit::message::Message to data to archive
    void archive(eckit::message::Message msg);

    /// Archives a stream of one or more messages.
    ///
    /// Reads messages from the eckit::DatAaHandle and calls archive() on the corresponding messages.
    /// Any callback set with registerArchiveCallback will be invoked on each message.
    /// @param handle eckit::DataHandle reference data to archive
    void archive(eckit::DataHandle& handle);

    /// Archive binary data to a FDB.
    ///
    /// Internally creates a DataHandle and calls archive().
    /// Any callback set with registerArchiveCallback will be invoked on each message.
    /// @param data Pointer to the binary data to archive
    /// @param length Size of the data to archive with the given
    void archive(const void* data, size_t length);

    /// Archives data from Datahandle and ensures all keys exactly match the provided MarsRequest.
    ///
    /// Any callback set with registerArchiveCallback will be invoked on each message.
    /// @param request a mars request
    /// @param handle a data handle pointing to the data
    /// @throws eckit::UserError if there are more keys in the MarsRequest then in the messages.
    /// @throws eckit::UserError if message key not present in MarsRequest.
    void archive(const metkit::mars::MarsRequest& request, eckit::DataHandle& handle);

    /// Archive a binary blob into FDB.
    ///
    /// Any callback set with registerArchiveCallback will be invoked.
    /// @note No constistency checks are applied. The caller needs to ensure the provided key matches metadata present
    /// in data.
    /// @param key Key used for indexing and archiving the data
    /// @param data Pointer to the binary blob to archive
    /// @param length Size in bytes of the binary blob to archive
    void archive(const Key& key, const void* data, size_t length);

    /// Generate an new index entry for an existing field location.
    ///
    /// Can be used to reindex existing data into a new catalogue (see fdb-reindex tool).
    /// @param key Key used to index the data.
    /// @param location Location of existing data in an FDB store.
    void reindex(const Key& key, const FieldLocation& location);

    /// Flush all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    // TODO(simondsmart): Review this. This is a bit odd. The purpose of a URI is that it directly describes the
    // data locations - and as such shouldn't need the FDB object to do the conversion into DataHandle?
    /// Read binary data from an URI.
    /// @param uri eckit uri to the data source
    /// @return DataHandle for reading the requested data from
    eckit::DataHandle* read(const eckit::URI& uri);

    /// Read binary data from an list of URI.
    /// @param vector of uris eckit uris to the data source
    /// @param inStorageOrder if set data will be returned in the order it is stored. If unset data will be returned in
    /// the order it was requested.
    /// @return DataHandle for reading the requested data
    eckit::DataHandle* read(const std::vector<eckit::URI>& uris, bool inStorageOrder = false);

    /// Read binary from a ListIterator.
    /// @param uris a list iterator which resembles a set of fields which should be read
    /// @param inStorageOrder if set data will be returned in the order it is stored. If unset data will be returned in
    /// the order it was requested.
    /// @return DataHandle for reading the requested data from
    eckit::DataHandle* read(ListIterator& it, bool inStorageOrder = false);

    /// Retrieve data which is specified by a MARS request.
    /// @param request MarsRequest which describes the data which should be retrieved
    /// @return DataHandle for reading the requested data from
    eckit::DataHandle* retrieve(const metkit::mars::MarsRequest& request);

    // TODO(kkratz): Provide doc!
    ListIterator inspect(const metkit::mars::MarsRequest& request);

    /// List data present at the archive and which can be retrieved.
    /// @param request FDBToolRequest stating which data should be queried
    /// @param deduplicate bool whether the returned iterator should ignore duplicates
    /// @param length Size of the data to archive with the given @p key
    /// @return ListIterator for iterating over the set of found items
    ListIterator list(const FDBToolRequest& request, bool deduplicate = false, int level = 3);

    /// Dump the structural content of the FDB
    ///
    /// In particular, in the TOC formulation, enumerate the different entries
    /// in the Table of Contents (including INIT and CLEAR entries).
    /// The dump will include information identifying the data files that are
    /// referenced, and the "Axes" which describe the maximum possible extent of
    /// the data that is contained in the database.
    /// @param request
    /// @param simple
    /// @return DumpIterator for iterating over the set of found items
    DumpIterator dump(const FDBToolRequest& request, bool simple = false);

    // TODO(kkratz): Provide doc!
    StatusIterator status(const FDBToolRequest& request);

    /// Wipe data from the database.
    ///
    /// Deletes FDB databases and the data therein contained. Uses the passed
    /// request to identify the database to delete. This is equivalent to a
    /// UNIX rm command.
    /// This tool deletes either whole databases, or whole indexes within databases
    /// @param request FDBToolRequest stating which data should be queried
    /// @param doit flag for committing to the wipe (default is dry-run)
    /// @param porcelain flag print only a list of files to be deleted / that are deleted
    /// @param unsafeWipeAll flag for omitting all security checks and force a wipe
    /// @return WipeIterator for iterating over the set of wiped items
    WipeIterator wipe(const FDBToolRequest& request, bool doit = false, bool porcelain = false,
                      bool unsafeWipeAll = false);

    /// Move content of one FDB database.
    ///
    /// This locks the source database, make it possible to create a second
    /// database in another root, duplicates all data. Source data are not automatically removed.
    /// @param request a fdb tool request for the data which should be move
    /// @param dest destination uri to which the data should be moved
    /// @return MoveIterator for iterating over the set of found items
    MoveIterator move(const FDBToolRequest& request, const eckit::URI& dest);

    /// Remove duplicate data from the database.
    ///
    /// Purge duplicate entries from the database and remove the associated data if the data is owned and not adopted.
    /// Data in the FDB5 is immutable. It is masked, but not removed, when overwritten with new data using the same key.
    /// Masked data can no longer be accessed. Indexes and data files that only contains masked data may be removed.
    /// If an index refers to data that is not owned by the FDB (in particular data which has been adopted from an
    /// existing FDB5), this data will not be removed.
    /// @param request a fdb tool request for the data which should be purged
    /// @param doit bool if true the purge is triggered, otherwise a dry-run is executed
    /// @param porcelain bool for printing only those files which are deleted
    /// @return PurgeIterator for iterating over the set of found items
    PurgeIterator purge(const FDBToolRequest& request, bool doit = false, bool porcelain = false);

    /// Prints information about FDB databases, aggregating the
    /// information over all the databases visited into a final summary.
    /// @param request FDB tool request for which the stats should be shown
    /// @return StatsIterator for iterating over the set of found items
    StatsIterator stats(const FDBToolRequest& request);

    // TODO(kkratz): Provide doc!
    /// @param request FDB tool request
    /// @param action control action
    /// @param identifiers identifiers
    /// @return ControlIterator for iterating over the set of found items
    ControlIterator control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers);

    // TODO(kkratz): Provide doc!
    /// @param request FDB tool request
    /// @param level maximum level the axis visitor should respect
    /// @return IndexAxis
    IndexAxis axes(const FDBToolRequest& request, int level = 3);

    // TODO(kkratz): Provide doc!
    /// @param request FDB tool request
    /// @param level maximum level the axis visitor should respect
    /// @return AxisIterator
    AxesIterator axesIterator(const FDBToolRequest& request, int level = 3);

    /// Check whether a specific control identifier is enabled
    /// @param controlIdentifier a given control identifier
    /// @return bool true or false, depending on the internal status of the FDB
    bool enabled(const ControlIdentifier& controlIdentifier) const;

    /// Return whether a flush of the FDB is needed
    /// @return true if an archive has happened and a flush is needed
    bool dirty() const;

    /// Register an archive callback.
    /// @param callback an archive callback which should be triggered during archive
    void registerArchiveCallback(ArchiveCallback callback);

    /// Register a flush callback.
    /// @param callback an flush callback which should be triggered during flushing
    void registerFlushCallback(FlushCallback callback);

    // -------------- API management -----------------------------------------------------------------------------------

    /// Returns the coniguration serialised into a string.
    /// @return config as string.
    const std::string id() const;

    // TODO(kkratz): Provide doc!
    FDBStats stats() const;

    /// Type of FDB, local or remote
    /// @return name of FDB type used, 'local' or 'remote'
    const std::string& name() const;

    /// Read-only access to this FDBs configuration.
    /// @return configuration
    const Config& config() const;

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
