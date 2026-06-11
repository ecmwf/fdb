// fdb_bridge.cpp - C++ bridge implementation
//
// This file implements the shim functions that convert between the native
// FDB5 C++ API and cxx-compatible types.

// trycatch handler — must come before the cxx-generated header so the
// generated wrappers' Result<T> handling picks up our specialization.
#include "fdb_exceptions.h"

#include "fdb_bridge.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/fdb5_version.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/runtime/Main.h"
#include "metkit/mars/MarsRequest.h"

#include <mutex>
#include <sstream>
#include <stdexcept>

// Include cxx-generated headers for bridge types
#include "fdb-sys/src/lib.rs.h"
#include "metkit-sys/src/lib.rs.h"

namespace fdb::ffi {

// ============================================================================
// Initialization
// ============================================================================

static std::once_flag init_flag;

void fdb_init() {
    std::call_once(init_flag, []() {
        // Initialize eckit::Main if not already initialized
        if (!eckit::Main::ready()) {
            static const char* argv[] = {"fdb-sys", nullptr};
            eckit::Main::initialise(1, const_cast<char**>(argv));
        }
    });
}

// ============================================================================
// Helper functions for type conversion
// ============================================================================

/// Convert KeyData to fdb5::Key
static fdb5::Key to_fdb_key(const KeyData& data) {
    fdb5::Key key;
    for (const auto& entry : data.entries) {
        key.set(std::string(entry.key), std::string(entry.value));
    }
    return key;
}

/// Convert fdb5::Key to Vec<KeyValue>
static rust::Vec<KeyValue> from_fdb_key(const fdb5::Key& key) {
    rust::Vec<KeyValue> result;
    for (const auto& [k, v] : key) {
        KeyValue kv;
        kv.key = rust::String(k);
        kv.value = rust::String(v);
        result.push_back(std::move(kv));
    }
    return result;
}

// ============================================================================
// FdbHandle implementation
// ============================================================================

FdbHandle::FdbHandle() = default;

FdbHandle::FdbHandle(const eckit_bridge::ConfigWrapper& config) : impl_(fdb5::Config(config.inner())) {}

FdbHandle::FdbHandle(const eckit_bridge::ConfigWrapper& config, const eckit_bridge::ConfigWrapper& user_config) :
    impl_(fdb5::Config(config.inner(), user_config.inner())) {}

FdbHandle::~FdbHandle() = default;

bool FdbHandle::dirty() const {
    return impl_.dirty();
}

void FdbHandle::flush() {
    impl_.flush();
}

FdbStatsData FdbHandle::stats() const {
    auto s = impl_.stats();
    FdbStatsData data;
    data.num_archive = s.numArchive();
    data.num_location = s.numLocation();
    data.num_flush = s.numFlush();
    return data;
}

bool FdbHandle::enabled(fdb5::ControlIdentifier identifier) const {
    return impl_.enabled(identifier);
}

rust::String FdbHandle::id() const {
    return rust::String(impl_.id());
}

rust::String FdbHandle::name() const {
    return rust::String(impl_.name());
}


// ============================================================================
// ListIteratorHandle implementation
// ============================================================================

ListIteratorHandle::ListIteratorHandle(fdb5::ListIterator&& it) : impl_(std::move(it)) {}

ListIteratorHandle::~ListIteratorHandle() = default;

bool ListIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    // Try to fetch next element
    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

ListElementData ListIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    ListElementData data;
    // Use `fullUri()` (not `uri()`) so the resulting string encodes the
    // entry's offset in the URI fragment and its length in the `length` query
    // parameter. This matches what `FieldLocation(const eckit::URI&)` parses
    // back, so the URI is round-trippable through `read_uri()` without the
    // caller having to seek manually. Same pattern as the upstream
    // `fdb-url`/`fdb-hammer` tools.
    data.uri = rust::String(current_.location().fullUri().asRawString());
    data.offset = current_.location().offset();
    data.length = current_.location().length();

    // Extract keys
    const auto& keys = current_.keys();
    if (keys.size() > 0) {
        data.db_key = from_fdb_key(keys[0]);
    }
    if (keys.size() > 1) {
        data.index_key = from_fdb_key(keys[1]);
    }
    if (keys.size() > 2) {
        data.datum_key = from_fdb_key(keys[2]);
    }

    // Convert timestamp to epoch seconds
    data.timestamp = static_cast<int64_t>(current_.timestamp());

    return data;
}

// ============================================================================
// DumpIteratorHandle implementation
// ============================================================================

DumpIteratorHandle::DumpIteratorHandle(fdb5::DumpIterator&& it) : impl_(std::move(it)) {}

DumpIteratorHandle::~DumpIteratorHandle() = default;

bool DumpIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

DumpElementData DumpIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    DumpElementData data;
    // DumpElement is a string
    data.content = rust::String(current_);
    return data;
}

// ============================================================================
// StatusIteratorHandle implementation
// ============================================================================

StatusIteratorHandle::StatusIteratorHandle(fdb5::StatusIterator&& it) : impl_(std::move(it)) {}

StatusIteratorHandle::~StatusIteratorHandle() = default;

bool StatusIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

StatusElementData StatusIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    StatusElementData data;
    data.location = rust::String(current_.location.asString());
    return data;
}

// ============================================================================
// WipeIteratorHandle implementation
// ============================================================================

WipeIteratorHandle::WipeIteratorHandle(fdb5::WipeIterator&& it) : impl_(std::move(it)) {}

WipeIteratorHandle::~WipeIteratorHandle() = default;

bool WipeIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

WipeElementData WipeIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    WipeElementData data;
    std::ostringstream ss;
    ss << current_;
    data.content = rust::String(ss.str());
    return data;
}

// ============================================================================
// PurgeIteratorHandle implementation
// ============================================================================

PurgeIteratorHandle::PurgeIteratorHandle(fdb5::PurgeIterator&& it) : impl_(std::move(it)) {}

PurgeIteratorHandle::~PurgeIteratorHandle() = default;

bool PurgeIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

PurgeElementData PurgeIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    PurgeElementData data;
    std::ostringstream ss;
    ss << current_;
    data.content = rust::String(ss.str());
    return data;
}

// ============================================================================
// StatsIteratorHandle implementation
// ============================================================================

StatsIteratorHandle::StatsIteratorHandle(fdb5::StatsIterator&& it) : impl_(std::move(it)) {}

StatsIteratorHandle::~StatsIteratorHandle() = default;

bool StatsIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

StatsElementData StatsIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    // Mirror `fdb5::StatsElement { IndexStats; DbStats; }` directly.
    // For `IndexStats` we can read every numeric accessor; for
    // `DbStats` upstream only exposes `report(ostream&)`, so the
    // captured text is the only thing we can surface.
    StatsElementData data;
    data.index_statistics.fields_count = current_.indexStatistics.fieldsCount();
    data.index_statistics.fields_size = current_.indexStatistics.fieldsSize();
    data.index_statistics.duplicates_count = current_.indexStatistics.duplicatesCount();
    data.index_statistics.duplicates_size = current_.indexStatistics.duplicatesSize();
    {
        std::ostringstream os;
        current_.indexStatistics.report(os);
        data.index_statistics.report = os.str();
    }
    {
        std::ostringstream os;
        current_.dbStatistics.report(os);
        data.db_statistics.report = os.str();
    }
    return data;
}

// ============================================================================
// ControlIteratorHandle implementation
// ============================================================================

ControlIteratorHandle::ControlIteratorHandle(fdb5::ControlIterator&& it) : impl_(std::move(it)) {}

ControlIteratorHandle::~ControlIteratorHandle() = default;

bool ControlIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    else {
        exhausted_ = true;
        return false;
    }
}

ControlElementData ControlIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    ControlElementData data;
    data.location = rust::String(current_.location.asString());
    for (const auto& id : current_.controlIdentifiers) {
        data.identifiers.push_back(id);
    }
    return data;
}

// ============================================================================
// Library metadata functions
// ============================================================================

rust::String fdb_version() {
    return rust::String(fdb5_version_str());
}

rust::String fdb_git_sha1() {
    return rust::String(fdb5_git_sha1());
}


// ============================================================================
// Handle lifecycle functions
// ============================================================================

std::unique_ptr<FdbHandle> new_fdb() {
    return std::make_unique<FdbHandle>();
}

std::unique_ptr<FdbHandle> new_fdb_from_config(const eckit_bridge::ConfigWrapper& config) {
    return std::make_unique<FdbHandle>(config);
}

std::unique_ptr<FdbHandle> new_fdb_from_config_with_user_config(const eckit_bridge::ConfigWrapper& config,
                                                                const eckit_bridge::ConfigWrapper& user_config) {
    return std::make_unique<FdbHandle>(config, user_config);
}

// ============================================================================
// Archive functions
// ============================================================================

void archive(FdbHandle& handle, const KeyData& key, rust::Slice<const uint8_t> data) {
    fdb5::Key fdb_key = to_fdb_key(key);
    handle.inner().archive(fdb_key, data.data(), data.size());
}

void archive_raw(FdbHandle& handle, rust::Slice<const uint8_t> data) {
    handle.inner().archive(data.data(), data.size());
}

namespace {

/// `eckit::DataHandle` adapter that pulls bytes from a Rust `std::io::Read`
/// source via the cxx callback `invoke_reader_read`. Used by
/// `archive_reader` to stream Rust-side data into
/// `fdb5::FDB::archive(eckit::DataHandle&)` without buffering the whole
/// payload in memory first.
///
/// Only the methods that `fdb5::FDB::archive(DataHandle&)` actually
/// touches are overridden — `openForRead`, `read`, `close`, `estimate`,
/// `size`, plus the abstract `print`. Everything else inherits the base
/// behaviour (which throws `NotImplemented` for the seek/write paths
/// `archive` never reaches).
class RustReaderHandle : public eckit::DataHandle {
public:

    explicit RustReaderHandle(rust::Box<ReaderBox> reader) : reader_(std::move(reader)) {}

    void print(std::ostream& s) const override { s << "RustReaderHandle[]"; }

    eckit::Length openForRead() override { return eckit::Length(0); }

    long read(void* buffer, long length) override {
        if (length <= 0) {
            return 0;
        }
        auto* bytes = static_cast<uint8_t*>(buffer);
        rust::Slice<uint8_t> slice{bytes, static_cast<size_t>(length)};
        int64_t n = invoke_reader_read(*reader_, slice);
        if (n < 0) {
            throw eckit::ReadError("RustReaderHandle: error reading from Rust source");
        }
        return static_cast<long>(n);
    }

    void close() override {}

    eckit::Length estimate() override { return eckit::Length(0); }

    eckit::Length size() override { return eckit::Length(0); }

private:

    rust::Box<ReaderBox> reader_;
};

}  // namespace

void archive_reader(FdbHandle& handle, rust::Box<ReaderBox> reader) {
    RustReaderHandle adapter(std::move(reader));
    handle.inner().archive(adapter);
}

MessageArchiverWrapper::MessageArchiverWrapper(const KeyData& key, bool complete_transfers, bool verbose,
                                               const eckit_bridge::ConfigWrapper& config) :
    archiver_(to_fdb_key(key), complete_transfers, verbose, fdb5::Config(config.inner())) {}

int64_t MessageArchiverWrapper::archive(eckit_bridge::DataHandleWrapper& source) {
    auto length = archiver_.archive(source.inner());
    return static_cast<int64_t>(static_cast<long long>(length));
}

void MessageArchiverWrapper::flush() {
    archiver_.flush();
}

std::unique_ptr<MessageArchiverWrapper> new_message_archiver(const KeyData& key, bool complete_transfers, bool verbose,
                                                             const eckit_bridge::ConfigWrapper& config) {
    return std::make_unique<MessageArchiverWrapper>(key, complete_transfers, verbose, config);
}

// ============================================================================
// Retrieve functions
// ============================================================================

std::unique_ptr<DataHandleWrapper> retrieve(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request) {
    return std::make_unique<DataHandleWrapper>(handle.inner().retrieve(request.inner()));
}

// ============================================================================
// Read functions (by URI)
// ============================================================================

std::unique_ptr<DataHandleWrapper> read_uri(FdbHandle& handle, rust::Str uri) {
    std::string uri_str{uri};
    eckit::URI eckit_uri{uri_str};
    return std::make_unique<DataHandleWrapper>(handle.inner().read(eckit_uri));
}

std::unique_ptr<DataHandleWrapper> read_uris(FdbHandle& handle, const rust::Vec<rust::String>& uris,
                                             bool in_storage_order) {
    std::vector<eckit::URI> eckit_uris;
    eckit_uris.reserve(uris.size());
    for (const auto& uri : uris) {
        eckit_uris.emplace_back(std::string(uri));
    }
    return std::make_unique<DataHandleWrapper>(handle.inner().read(eckit_uris, in_storage_order));
}

std::unique_ptr<DataHandleWrapper> read_list_iterator(FdbHandle& handle, ListIteratorHandle& iterator,
                                                      bool in_storage_order) {
    return std::make_unique<DataHandleWrapper>(handle.inner().read(iterator.inner(), in_storage_order));
}

// ============================================================================
// List functions
// ============================================================================

std::unique_ptr<ListIteratorHandle> list(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request,
                                         bool deduplicate, int32_t level) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().list(tool_request, deduplicate, level);
    return std::make_unique<ListIteratorHandle>(std::move(it));
}

CompactListingData list_iterator_dump_compact(ListIteratorHandle& iterator) {
    std::ostringstream os;
    auto [fields, length] = iterator.inner().dumpCompact(os);
    CompactListingData data;
    data.text = rust::String(os.str());
    data.fields = static_cast<uint64_t>(fields);
    data.total_bytes = static_cast<uint64_t>(length);
    return data;
}

// ============================================================================
// Axes query functions
// ============================================================================

rust::Vec<AxisEntry> axes(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request, int32_t level) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto index_axis = handle.inner().axes(tool_request, level);

    rust::Vec<AxisEntry> result;
    // Iterate over all axes using map() instead of hardcoded list
    auto axes_map = index_axis.map();
    for (const auto& [axis_name, values_set] : axes_map) {
        AxisEntry entry;
        entry.key = rust::String(axis_name);
        for (const auto& v : values_set) {
            entry.values.push_back(rust::String(v));
        }
        result.push_back(std::move(entry));
    }
    return result;
}

// ============================================================================
// Dump functions
// ============================================================================

std::unique_ptr<DumpIteratorHandle> dump(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request,
                                         bool simple) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().dump(tool_request, simple);
    return std::make_unique<DumpIteratorHandle>(std::move(it));
}

// ============================================================================
// Status functions
// ============================================================================

std::unique_ptr<StatusIteratorHandle> status(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().status(tool_request);
    return std::make_unique<StatusIteratorHandle>(std::move(it));
}

// ============================================================================
// Wipe functions
// ============================================================================

std::unique_ptr<WipeIteratorHandle> wipe(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request, bool doit,
                                         bool porcelain, bool unsafe_wipe_all) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().wipe(tool_request, doit, porcelain, unsafe_wipe_all);
    return std::make_unique<WipeIteratorHandle>(std::move(it));
}

// ============================================================================
// Purge functions
// ============================================================================

std::unique_ptr<PurgeIteratorHandle> purge(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request,
                                           bool doit, bool porcelain) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().purge(tool_request, doit, porcelain);
    return std::make_unique<PurgeIteratorHandle>(std::move(it));
}

// ============================================================================
// Stats functions
// ============================================================================

std::unique_ptr<StatsIteratorHandle> stats_iterator(FdbHandle& handle,
                                                    const metkit_bridge::MarsRequestWrapper& request) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = handle.inner().stats(tool_request);
    return std::make_unique<StatsIteratorHandle>(std::move(it));
}

// ============================================================================
// Control functions
// ============================================================================

std::unique_ptr<ControlIteratorHandle> control(FdbHandle& handle, const metkit_bridge::MarsRequestWrapper& request,
                                               fdb5::ControlAction action,
                                               rust::Slice<const fdb5::ControlIdentifier> identifiers) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};

    fdb5::ControlIdentifiers ctrl_ids;
    for (auto id : identifiers) {
        ctrl_ids |= id;
    }

    auto it = handle.inner().control(tool_request, action, ctrl_ids);
    return std::make_unique<ControlIteratorHandle>(std::move(it));
}

// ============================================================================
// Callback registration functions
// ============================================================================

void register_flush_callback(FdbHandle& handle, rust::Box<FlushCallbackBox> callback) {
    // Create a shared_ptr to hold the callback box so it can be captured by the lambda
    auto callback_ptr = std::make_shared<rust::Box<FlushCallbackBox>>(std::move(callback));

    fdb5::FlushCallback cpp_callback = [callback_ptr]() { invoke_flush_callback(**callback_ptr); };

    handle.inner().registerFlushCallback(std::move(cpp_callback));
}

void register_archive_callback(FdbHandle& handle, rust::Box<ArchiveCallbackBox> callback) {
    // Create a shared_ptr to hold the callback box so it can be captured by the lambda
    auto callback_ptr = std::make_shared<rust::Box<ArchiveCallbackBox>>(std::move(callback));

    fdb5::ArchiveCallback cpp_callback = [callback_ptr](
                                             const fdb5::Key& key, const void* data, size_t length,
                                             std::future<std::shared_ptr<const fdb5::FieldLocation>> location_future) {
        // Convert key to Vec<KeyValue>
        rust::Vec<KeyValue> key_vec;
        for (const auto& [k, v] : key) {
            KeyValue kv;
            kv.key = rust::String(k);
            kv.value = rust::String(v);
            key_vec.push_back(std::move(kv));
        }

        // Create a slice from the data
        rust::Slice<const uint8_t> data_slice{static_cast<const uint8_t*>(data), length};

        // Wait for the location future and extract info
        std::string location_uri;
        uint64_t location_offset = 0;
        uint64_t location_length = 0;

        try {
            auto location = location_future.get();
            if (location) {
                location_uri = location->uri().asRawString();
                location_offset = location->offset();
                location_length = location->length();
            }
        }
        catch (const std::exception&) {
            // If future fails, leave location info empty (best-effort)
        }

        // Create a slice from key_vec
        rust::Slice<const KeyValue> key_slice{key_vec.data(), key_vec.size()};

        invoke_archive_callback(**callback_ptr, key_slice, data_slice, rust::Str(location_uri), location_offset,
                                location_length);
    };

    handle.inner().registerArchiveCallback(std::move(cpp_callback));
}

// ============================================================================
// Test functions (for verifying exception handling)
// ============================================================================

void test_throw_eckit_exception() {
    throw eckit::Exception("test eckit exception");
}

void test_throw_eckit_serious_bug() {
    throw eckit::SeriousBug("test serious bug");
}

void test_throw_eckit_user_error() {
    throw eckit::UserError("test user error");
}

void test_throw_std_exception() {
    throw std::runtime_error("test std exception");
}

void test_throw_int() {
    throw 42;
}

}  // namespace fdb::ffi
