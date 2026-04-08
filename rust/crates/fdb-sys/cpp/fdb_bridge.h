// fdb_bridge.h - C++ bridge declarations for cxx
//
// This header declares wrapper types and shim functions that convert between
// the native FDB5 C++ API and cxx-compatible types.

#pragma once

#include "rust/cxx.h"

#include <memory>
#include <string>
#include <vector>

// Include eckit exception for the global trycatch handler
#include "eckit/exception/Exceptions.h"

// Custom exception handler for cxx - catches eckit exceptions globally
// This replaces per-function try-catch blocks throughout the bridge
// Exception messages are prefixed with type for Rust-side discrimination
// Order matters: catch specific exceptions before base classes
namespace rust::behavior {
template <typename Try, typename Fail>
static void trycatch(Try&& func, Fail&& fail) noexcept try {
    func();
}
catch (const eckit::SeriousBug& e) {
    fail((std::string("ECKIT_SERIOUS_BUG: ") + e.what()).c_str());
}
catch (const eckit::UserError& e) {
    fail((std::string("ECKIT_USER_ERROR: ") + e.what()).c_str());
}
catch (const eckit::BadParameter& e) {
    fail((std::string("ECKIT_BAD_PARAMETER: ") + e.what()).c_str());
}
catch (const eckit::NotImplemented& e) {
    fail((std::string("ECKIT_NOT_IMPLEMENTED: ") + e.what()).c_str());
}
catch (const eckit::OutOfRange& e) {
    fail((std::string("ECKIT_OUT_OF_RANGE: ") + e.what()).c_str());
}
catch (const eckit::FileError& e) {
    fail((std::string("ECKIT_FILE_ERROR: ") + e.what()).c_str());
}
catch (const eckit::AssertionFailed& e) {
    fail((std::string("ECKIT_ASSERTION_FAILED: ") + e.what()).c_str());
}
catch (const eckit::Exception& e) {
    fail((std::string("ECKIT: ") + e.what()).c_str());
}
catch (const std::exception& e) {
    fail(e.what());
}
// REQUIRED: catch(...) is necessary at FFI boundary to prevent undefined behavior.
catch (...) {
    fail("unknown C++ exception (non-std::exception type)");
}
}  // namespace rust::behavior

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"

#include "eckit/io/DataHandle.h"

namespace fdb::ffi {

// ============================================================================
// Shared struct forward declarations (defined by cxx in generated code)
// ============================================================================

struct KeyValue;
struct KeyData;
struct RequestData;
struct ListElementData;
struct AxisEntry;
struct FdbStatsData;
struct DumpElementData;
struct StatusElementData;
struct WipeElementData;
struct PurgeElementData;
struct StatsElementData;
struct ControlElementData;
struct MoveElementData;
struct ConfigData;

// ============================================================================
// Wrapper classes for opaque C++ types
// ============================================================================

/// Wrapper around fdb5::FDB that can be passed through cxx.
class FdbHandle {
public:

    FdbHandle();
    explicit FdbHandle(const std::string& yaml_config);
    FdbHandle(const std::string& yaml_config, const std::string& yaml_user_config);
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

    // -------------------------------------------------------------------------
    // Methods exposed to Rust via cxx
    // -------------------------------------------------------------------------

    /// Check if the FDB has unflushed data.
    bool dirty() const;

    /// Flush pending writes to disk.
    void flush();

    /// Get aggregate statistics.
    FdbStatsData stats() const;

    /// Check if a control identifier is enabled.
    bool enabled(fdb5::ControlIdentifier identifier) const;

    /// Get the FDB configuration ID.
    rust::String id() const;

    /// Get the FDB type name.
    rust::String name() const;

    /// Get the FDB configuration data.
    ConfigData config() const;

    /// Get a string value from the FDB configuration.
    rust::String config_string(rust::Str key) const;

    /// Get an integer value from the FDB configuration.
    int64_t config_int(rust::Str key) const;

    /// Get a boolean value from the FDB configuration.
    bool config_bool(rust::Str key) const;

    /// Check if a key exists in the FDB configuration.
    bool config_has(rust::Str key) const;

private:

    fdb5::FDB impl_;
};

/// Wrapper around eckit::DataHandle for reading retrieved data.
class DataReaderHandle {
public:

    explicit DataReaderHandle(std::unique_ptr<eckit::DataHandle> handle);
    ~DataReaderHandle();

    // Non-copyable
    DataReaderHandle(const DataReaderHandle&) = delete;
    DataReaderHandle& operator=(const DataReaderHandle&) = delete;

    // Movable
    DataReaderHandle(DataReaderHandle&&) = default;
    DataReaderHandle& operator=(DataReaderHandle&&) = default;

    // -------------------------------------------------------------------------
    // Methods exposed to Rust via cxx
    // -------------------------------------------------------------------------

    void open();
    void close();
    size_t read(rust::Slice<uint8_t> buffer);
    void seek(uint64_t position);
    uint64_t tell() const;
    uint64_t size() const;

private:

    std::unique_ptr<eckit::DataHandle> impl_;
    bool is_open_ = false;
};

/// Wrapper around fdb5::ListIterator.
class ListIteratorHandle {
public:

    explicit ListIteratorHandle(fdb5::ListIterator&& it);
    ~ListIteratorHandle();

    // Non-copyable
    ListIteratorHandle(const ListIteratorHandle&) = delete;
    ListIteratorHandle& operator=(const ListIteratorHandle&) = delete;

    // Movable
    ListIteratorHandle(ListIteratorHandle&&) = default;
    ListIteratorHandle& operator=(ListIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    ListElementData next();

    /// Access the underlying ListIterator (for read_list_iterator).
    fdb5::ListIterator& inner() { return impl_; }

private:

    fdb5::ListIterator impl_;
    fdb5::ListElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::DumpIterator.
class DumpIteratorHandle {
public:

    explicit DumpIteratorHandle(fdb5::DumpIterator&& it);
    ~DumpIteratorHandle();

    DumpIteratorHandle(const DumpIteratorHandle&) = delete;
    DumpIteratorHandle& operator=(const DumpIteratorHandle&) = delete;
    DumpIteratorHandle(DumpIteratorHandle&&) = default;
    DumpIteratorHandle& operator=(DumpIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    DumpElementData next();

private:

    fdb5::DumpIterator impl_;
    fdb5::DumpElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::StatusIterator.
class StatusIteratorHandle {
public:

    explicit StatusIteratorHandle(fdb5::StatusIterator&& it);
    ~StatusIteratorHandle();

    StatusIteratorHandle(const StatusIteratorHandle&) = delete;
    StatusIteratorHandle& operator=(const StatusIteratorHandle&) = delete;
    StatusIteratorHandle(StatusIteratorHandle&&) = default;
    StatusIteratorHandle& operator=(StatusIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    StatusElementData next();

private:

    fdb5::StatusIterator impl_;
    fdb5::StatusElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::WipeIterator.
class WipeIteratorHandle {
public:

    explicit WipeIteratorHandle(fdb5::WipeIterator&& it);
    ~WipeIteratorHandle();

    WipeIteratorHandle(const WipeIteratorHandle&) = delete;
    WipeIteratorHandle& operator=(const WipeIteratorHandle&) = delete;
    WipeIteratorHandle(WipeIteratorHandle&&) = default;
    WipeIteratorHandle& operator=(WipeIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    WipeElementData next();

private:

    fdb5::WipeIterator impl_;
    fdb5::WipeElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::PurgeIterator.
class PurgeIteratorHandle {
public:

    explicit PurgeIteratorHandle(fdb5::PurgeIterator&& it);
    ~PurgeIteratorHandle();

    PurgeIteratorHandle(const PurgeIteratorHandle&) = delete;
    PurgeIteratorHandle& operator=(const PurgeIteratorHandle&) = delete;
    PurgeIteratorHandle(PurgeIteratorHandle&&) = default;
    PurgeIteratorHandle& operator=(PurgeIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    PurgeElementData next();

private:

    fdb5::PurgeIterator impl_;
    fdb5::PurgeElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::StatsIterator.
class StatsIteratorHandle {
public:

    explicit StatsIteratorHandle(fdb5::StatsIterator&& it);
    ~StatsIteratorHandle();

    StatsIteratorHandle(const StatsIteratorHandle&) = delete;
    StatsIteratorHandle& operator=(const StatsIteratorHandle&) = delete;
    StatsIteratorHandle(StatsIteratorHandle&&) = default;
    StatsIteratorHandle& operator=(StatsIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    StatsElementData next();

private:

    fdb5::StatsIterator impl_;
    fdb5::StatsElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::ControlIterator.
class ControlIteratorHandle {
public:

    explicit ControlIteratorHandle(fdb5::ControlIterator&& it);
    ~ControlIteratorHandle();

    ControlIteratorHandle(const ControlIteratorHandle&) = delete;
    ControlIteratorHandle& operator=(const ControlIteratorHandle&) = delete;
    ControlIteratorHandle(ControlIteratorHandle&&) = default;
    ControlIteratorHandle& operator=(ControlIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    ControlElementData next();

private:

    fdb5::ControlIterator impl_;
    fdb5::ControlElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

/// Wrapper around fdb5::MoveIterator.
class MoveIteratorHandle {
public:

    explicit MoveIteratorHandle(fdb5::MoveIterator&& it);
    ~MoveIteratorHandle();

    MoveIteratorHandle(const MoveIteratorHandle&) = delete;
    MoveIteratorHandle& operator=(const MoveIteratorHandle&) = delete;
    MoveIteratorHandle(MoveIteratorHandle&&) = default;
    MoveIteratorHandle& operator=(MoveIteratorHandle&&) = default;

    // Methods exposed to Rust via cxx
    bool hasNext();
    MoveElementData next();

private:

    fdb5::MoveIterator impl_;
    fdb5::MoveElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

// ============================================================================
// Initialization functions
// ============================================================================

/// Initialize the FDB library.
/// Must be called before any other FDB operations.
void fdb_init();

// ============================================================================
// Library metadata functions
// ============================================================================

/// Get the FDB library version string.
rust::String fdb_version();

/// Get the FDB git SHA1 hash.
rust::String fdb_git_sha1();

// ============================================================================
// Handle lifecycle functions
// ============================================================================

/// Create a new FDB handle with default configuration.
std::unique_ptr<FdbHandle> new_fdb();

/// Create a new FDB handle from YAML configuration.
std::unique_ptr<FdbHandle> new_fdb_from_yaml(rust::Str config);

/// Create a new FDB handle from YAML configuration plus a YAML "user config"
/// (per-instance overrides such as `useSubToc`, `preloadTocBTree`, etc.).
std::unique_ptr<FdbHandle> new_fdb_from_yaml_with_user_config(rust::Str config, rust::Str user_config);

// ============================================================================
// Archive functions
// ============================================================================

/// Archive data with an explicit key.
void archive(FdbHandle& handle, const KeyData& key, rust::Slice<const uint8_t> data);

/// Archive raw GRIB data (key is extracted from the message).
void archive_raw(FdbHandle& handle, rust::Slice<const uint8_t> data);

// ============================================================================
// Retrieve functions
// ============================================================================

/// Retrieve data matching a request.
std::unique_ptr<DataReaderHandle> retrieve(FdbHandle& handle, rust::Str request);

// ============================================================================
// Read functions (by URI)
// ============================================================================

/// Read data from a single URI.
std::unique_ptr<DataReaderHandle> read_uri(FdbHandle& handle, rust::Str uri);

/// Read data from a list of URIs.
std::unique_ptr<DataReaderHandle> read_uris(FdbHandle& handle, const rust::Vec<rust::String>& uris,
                                            bool in_storage_order);

/// Read data from a list iterator (most efficient - avoids URI conversion).
std::unique_ptr<DataReaderHandle> read_list_iterator(FdbHandle& handle, ListIteratorHandle& iterator,
                                                     bool in_storage_order);

// ============================================================================
// List functions
// ============================================================================

/// List data matching a request.
std::unique_ptr<ListIteratorHandle> list(FdbHandle& handle, rust::Str request, bool deduplicate, int32_t level);

// ============================================================================
// Axes query functions
// ============================================================================

/// Get axes for a request.
rust::Vec<AxisEntry> axes(FdbHandle& handle, rust::Str request, int32_t level);

// ============================================================================
// Dump functions
// ============================================================================

/// Dump database structure.
std::unique_ptr<DumpIteratorHandle> dump(FdbHandle& handle, rust::Str request, bool simple);

// ============================================================================
// Status functions
// ============================================================================

/// Get database status.
std::unique_ptr<StatusIteratorHandle> status(FdbHandle& handle, rust::Str request);

// ============================================================================
// Wipe functions
// ============================================================================

/// Wipe data matching a request.
std::unique_ptr<WipeIteratorHandle> wipe(FdbHandle& handle, rust::Str request, bool doit, bool porcelain,
                                         bool unsafe_wipe_all);

// ============================================================================
// Purge functions
// ============================================================================

/// Purge duplicate data.
std::unique_ptr<PurgeIteratorHandle> purge(FdbHandle& handle, rust::Str request, bool doit, bool porcelain);

// ============================================================================
// Stats functions
// ============================================================================

/// Get statistics iterator.
std::unique_ptr<StatsIteratorHandle> stats_iterator(FdbHandle& handle, rust::Str request);

// ============================================================================
// Control functions
// ============================================================================

/// Control database features.
std::unique_ptr<ControlIteratorHandle> control(FdbHandle& handle, rust::Str request, fdb5::ControlAction action,
                                               rust::Slice<const fdb5::ControlIdentifier> identifiers);

// ============================================================================
// Move functions
// ============================================================================

/// Move data to a new location.
std::unique_ptr<MoveIteratorHandle> move_data(FdbHandle& handle, rust::Str request, rust::Str dest);

// ============================================================================
// Callback registration functions
// ============================================================================

// Forward declare Rust callback box types
struct FlushCallbackBox;
struct ArchiveCallbackBox;

/// Register a flush callback.
void register_flush_callback(FdbHandle& handle, rust::Box<FlushCallbackBox> callback);

/// Register an archive callback.
void register_archive_callback(FdbHandle& handle, rust::Box<ArchiveCallbackBox> callback);

// ============================================================================
// Test functions (for verifying exception handling)
// ============================================================================

/// Test function that throws eckit::Exception
void test_throw_eckit_exception();

/// Test function that throws eckit::SeriousBug
void test_throw_eckit_serious_bug();

/// Test function that throws eckit::UserError
void test_throw_eckit_user_error();

/// Test function that throws std::runtime_error
void test_throw_std_exception();

/// Test function that throws an int (non-std::exception type)
void test_throw_int();

}  // namespace fdb::ffi
