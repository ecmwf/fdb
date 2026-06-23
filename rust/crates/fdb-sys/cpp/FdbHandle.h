// fdb FDB bridge — wraps `fdb5::FDB`.
#pragma once

#include "Types.h"

#include "eckit-sys/src/lib.rs.h"   // ConfigWrapper / DataHandleWrapper
#include "metkit-sys/src/lib.rs.h"  // MarsRequestWrapper

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ControlIterator.h"

#include "rust/cxx.h"

#include <cstdint>
#include <memory>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

class ListIteratorHandle;
class DumpIteratorHandle;
class StatusIteratorHandle;
class WipeIteratorHandle;
class PurgeIteratorHandle;
class StatsIteratorHandle;
class ControlIteratorHandle;

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::FDB` for Rust FFI.
class FdbHandle {
public:

    FdbHandle();
    explicit FdbHandle(const eckit_bridge::ConfigWrapper& config);
    FdbHandle(const eckit_bridge::ConfigWrapper& config, const eckit_bridge::ConfigWrapper& user_config);

    ~FdbHandle();

    // Non-copyable
    FdbHandle(const FdbHandle&) = delete;
    FdbHandle& operator=(const FdbHandle&) = delete;

    // Movable
    FdbHandle(FdbHandle&&) = default;
    FdbHandle& operator=(FdbHandle&&) = default;

    /// Access the underlying FDB instance.
    fdb5::FDB& inner() { return impl_; }
    const fdb5::FDB& inner() const { return impl_; }

    // ============== Query / status ==============

    bool dirty() const;
    void flush();
    FdbStatsData stats() const;
    bool enabled(fdb5::ControlIdentifier identifier) const;
    rust::String id() const;
    rust::String name() const;

    // ============== Archive ==============

    /// Archive data with an explicit key.
    void archive(const KeyData& key, rust::Slice<const uint8_t> data);

    /// Archive raw GRIB data (key is extracted from the message).
    void archive_raw(rust::Slice<const uint8_t> data);

    /// Archive raw GRIB data streamed from a Rust `std::io::Read` source.
    void archive_reader(rust::Box<ReaderBox> reader);

    // ============== Retrieve / read ==============

    /// Retrieve data matching a MARS request.
    std::unique_ptr<eckit_bridge::DataHandleWrapper> retrieve(const metkit_bridge::MarsRequestWrapper& request);

    /// Read data from a single URI.
    std::unique_ptr<eckit_bridge::DataHandleWrapper> read_uri(rust::Str uri);

    /// Read data from a list of URIs.
    std::unique_ptr<eckit_bridge::DataHandleWrapper> read_uris(const rust::Vec<rust::String>& uris,
                                                               bool in_storage_order);

    /// Read data from a list iterator — avoids URI round-tripping.
    std::unique_ptr<eckit_bridge::DataHandleWrapper> read_list_iterator(ListIteratorHandle& iterator,
                                                                        bool in_storage_order);

    // ============== Query iterators ==============

    std::unique_ptr<ListIteratorHandle> list(const metkit_bridge::MarsRequestWrapper& request, bool deduplicate,
                                             int32_t level);
    rust::Vec<AxisEntry> axes(const metkit_bridge::MarsRequestWrapper& request, int32_t level);
    std::unique_ptr<DumpIteratorHandle> dump(const metkit_bridge::MarsRequestWrapper& request, bool simple);
    std::unique_ptr<StatusIteratorHandle> status(const metkit_bridge::MarsRequestWrapper& request);
    std::unique_ptr<WipeIteratorHandle> wipe(const metkit_bridge::MarsRequestWrapper& request, bool doit,
                                             bool porcelain, bool unsafe_wipe_all);
    std::unique_ptr<PurgeIteratorHandle> purge(const metkit_bridge::MarsRequestWrapper& request, bool doit,
                                               bool porcelain);
    std::unique_ptr<StatsIteratorHandle> stats_iterator(const metkit_bridge::MarsRequestWrapper& request);
    std::unique_ptr<ControlIteratorHandle> control(const metkit_bridge::MarsRequestWrapper& request,
                                                   fdb5::ControlAction action,
                                                   rust::Slice<const fdb5::ControlIdentifier> identifiers);

    // ============== Callbacks ==============

    void register_flush_callback(rust::Box<FlushCallbackBox> callback);
    void register_archive_callback(rust::Box<ArchiveCallbackBox> callback);

    // ============== Factories ==============

    /// Create a new FDB handle with default configuration.
    static std::unique_ptr<FdbHandle> create();

    /// Create a new FDB handle from an eckit config.
    static std::unique_ptr<FdbHandle> from_config(const eckit_bridge::ConfigWrapper& config);

    /// Create a new FDB handle from an eckit config with a user-config overlay.
    static std::unique_ptr<FdbHandle> from_config_with_user(const eckit_bridge::ConfigWrapper& config,
                                                            const eckit_bridge::ConfigWrapper& user_config);

private:

    fdb5::FDB impl_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
