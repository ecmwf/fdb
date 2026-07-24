// fdb_bridge.cpp - C++ bridge implementation
//
// This file implements the shim functions that convert between the native
// FDB5 C++ API and cxx-compatible types.

#include "fdb_bridge.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/fdb5_version.h"

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/runtime/Main.h"
#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParsedRequest.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include <mutex>
#include <sstream>
#include <stdexcept>

// Include the cxx-generated header for our bridge types
#include "fdb-sys/src/lib.rs.h"

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

/// Parse a MARS request string into a fully-expanded `metkit::mars::MarsRequest`.
///
/// Uses the same parser + expansion pipeline as upstream FDB tools (see
/// `fdb5::FDBToolRequest::requestsFromString`):
///
///   1. Prepend a dummy verb (`retrieve`) so `MarsParser` accepts the input.
///   2. Run `MarsParser::parse()` to produce a `MarsParsedRequest`.
///   3. Run `MarsExpansion::expand()` to apply `to`/`by` ranges, type
///      expansion, optional fields, etc.
///
/// An empty request string is returned as a default-constructed
/// `MarsRequest` (matches everything) without invoking the parser.
///
/// Throws on any parser/expansion error; the global `rust::behavior::trycatch`
/// turns the exception into a Rust `Result::Err`.
static metkit::mars::MarsRequest parse_to_mars_request(const std::string& request_str) {
    if (request_str.empty()) {
        return metkit::mars::MarsRequest{};
    }

    // MarsParser requires a verb at the start of the input. Use "retrieve"
    // as the canonical verb (matches what `FDBToolRequest::requestsFromString`
    // defaults to). The verb itself is discarded by MarsExpansion.
    std::string full = "retrieve," + request_str;
    std::istringstream in(full);
    metkit::mars::MarsParser parser(in);
    auto parsed = parser.parse();
    ASSERT(parsed.size() == 1);

    metkit::mars::MarsExpansion expand(/*inherit*/ false, /*strict*/ true);
    auto expanded = expand.expand(parsed);
    ASSERT(expanded.size() == 1);
    return std::move(expanded.front());
}

/// Create an `FDBToolRequest` from a MARS request string.
static fdb5::FDBToolRequest make_tool_request(const std::string& request_str) {
    auto mars = parse_to_mars_request(request_str);
    // If the request is empty, match all; otherwise filter by request.
    bool all = mars.empty();
    return fdb5::FDBToolRequest{mars, all, std::vector<std::string>{}};
}

// ============================================================================
// FdbHandle implementation
// ============================================================================

FdbHandle::FdbHandle() = default;

FdbHandle::FdbHandle(const std::string& yaml_config) :
    impl_([&] {
        eckit::YAMLConfiguration config(yaml_config);
        fdb5::Config fdb_config(config);
        return fdb5::FDB(fdb_config);
    }()) {}

FdbHandle::FdbHandle(const std::string& yaml_config, const std::string& yaml_user_config) :
    impl_([&] {
        eckit::YAMLConfiguration config(yaml_config);
        eckit::YAMLConfiguration user_config(yaml_user_config);
        fdb5::Config fdb_config(config, user_config);
        return fdb5::FDB(fdb_config);
    }()) {}

FdbHandle::FdbHandle(FromPathTag, const std::string& path) :
    impl_([&] {
        // `Config::make` loads YAML/JSON from the given path, expands
        // `~fdb` and `fdb_home` references, and returns a fully-resolved
        // `fdb5::Config`. This is the same entry point upstream FDB tools
        // use when handed a `--config-file` / `FDB_CONFIG_FILE`.
        return fdb5::FDB(fdb5::Config::make(eckit::PathName(path)));
    }()) {}

FdbHandle::FdbHandle(FromPathTag, const std::string& path, const std::string& yaml_user_config) :
    impl_([&] {
        eckit::YAMLConfiguration user_config(yaml_user_config);
        return fdb5::FDB(fdb5::Config::make(eckit::PathName(path), user_config));
    }()) {}

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
// eckit::DataHandle shim functions
// ============================================================================

uint64_t data_handle_open(eckit::DataHandle& handle) {
    return static_cast<uint64_t>(handle.openForRead());
}

void data_handle_close(eckit::DataHandle& handle) {
    handle.close();
}

size_t data_handle_read(eckit::DataHandle& handle, rust::Slice<uint8_t> buffer) {
    long n = handle.read(buffer.data(), static_cast<long>(buffer.size()));
    return n < 0 ? 0 : static_cast<size_t>(n);
}

void data_handle_seek(eckit::DataHandle& handle, uint64_t position) {
    handle.seek(eckit::Offset(position));
}

uint64_t data_handle_tell(eckit::DataHandle& handle) {
    return static_cast<uint64_t>(handle.position());
}

uint64_t data_handle_size(eckit::DataHandle& handle) {
    return static_cast<uint64_t>(handle.size());
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
// MARS request parsing
// ============================================================================

RequestData parse_mars_request(rust::Str request) {
    // Parsing requires eckit to be initialised (type registries, log levels,
    // etc.), but `parse_mars_request` is a free function that may be called
    // before the user constructs an `Fdb`. Make it self-sufficient.
    fdb_init();

    auto mars = parse_to_mars_request(std::string(request));

    RequestData out;
    for (const auto& key : mars.params()) {
        RequestParam param;
        param.key = rust::String(key);
        for (const auto& v : mars.values(key)) {
            param.values.push_back(rust::String(v));
        }
        out.params.push_back(std::move(param));
    }
    return out;
}

// ============================================================================
// Handle lifecycle functions
// ============================================================================

std::unique_ptr<FdbHandle> new_fdb() {
    return std::make_unique<FdbHandle>();
}

std::unique_ptr<FdbHandle> new_fdb_from_yaml(rust::Str config) {
    return std::make_unique<FdbHandle>(std::string(config));
}

std::unique_ptr<FdbHandle> new_fdb_from_yaml_with_user_config(rust::Str config, rust::Str user_config) {
    return std::make_unique<FdbHandle>(std::string(config), std::string(user_config));
}

std::unique_ptr<FdbHandle> new_fdb_from_path(rust::Str path) {
    return std::make_unique<FdbHandle>(FdbHandle::FromPathTag{}, std::string(path));
}

std::unique_ptr<FdbHandle> new_fdb_from_path_with_user_config(rust::Str path, rust::Str user_config) {
    return std::make_unique<FdbHandle>(FdbHandle::FromPathTag{}, std::string(path), std::string(user_config));
}

// ============================================================================
// Archive functions
// ============================================================================

void FdbHandle::archive(const KeyData& key, rust::Slice<const uint8_t> data) {
    fdb5::Key fdb_key = to_fdb_key(key);
    inner().archive(fdb_key, data.data(), data.size());
}

void FdbHandle::archive_raw(rust::Slice<const uint8_t> data) {
    inner().archive(data.data(), data.size());
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

void FdbHandle::archive_reader(rust::Box<ReaderBox> reader) {
    RustReaderHandle adapter(std::move(reader));
    inner().archive(adapter);
}

// ============================================================================
// Retrieve functions
// ============================================================================

std::unique_ptr<eckit::DataHandle> FdbHandle::retrieve(rust::Str request) {
    auto mars = parse_to_mars_request(std::string(request));
    return std::unique_ptr<eckit::DataHandle>(inner().retrieve(mars));
}

// ============================================================================
// Read functions (by URI)
// ============================================================================

std::unique_ptr<eckit::DataHandle> FdbHandle::read_uri(rust::Str uri) {
    std::string uri_str{uri};
    eckit::URI eckit_uri{uri_str};
    return std::unique_ptr<eckit::DataHandle>(inner().read(eckit_uri));
}

std::unique_ptr<eckit::DataHandle> FdbHandle::read_uris(const rust::Vec<rust::String>& uris, bool in_storage_order) {
    std::vector<eckit::URI> eckit_uris;
    eckit_uris.reserve(uris.size());
    for (const auto& uri : uris) {
        eckit_uris.emplace_back(std::string(uri));
    }
    return std::unique_ptr<eckit::DataHandle>(inner().read(eckit_uris, in_storage_order));
}

std::unique_ptr<eckit::DataHandle> FdbHandle::read_list_iterator(ListIteratorHandle& iterator, bool in_storage_order) {
    // Calls FDB::read(ListIterator&, bool) directly - most efficient path
    return std::unique_ptr<eckit::DataHandle>(inner().read(iterator.inner(), in_storage_order));
}

// ============================================================================
// List functions
// ============================================================================

std::unique_ptr<ListIteratorHandle> FdbHandle::list(rust::Str request, bool deduplicate, int32_t level) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().list(tool_request, deduplicate, level);
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

rust::Vec<AxisEntry> FdbHandle::axes(rust::Str request, int32_t level) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto index_axis = inner().axes(tool_request, level);

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

std::unique_ptr<DumpIteratorHandle> FdbHandle::dump(rust::Str request, bool simple) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().dump(tool_request, simple);
    return std::make_unique<DumpIteratorHandle>(std::move(it));
}

// ============================================================================
// Status functions
// ============================================================================

std::unique_ptr<StatusIteratorHandle> FdbHandle::status(rust::Str request) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().status(tool_request);
    return std::make_unique<StatusIteratorHandle>(std::move(it));
}

// ============================================================================
// Wipe functions
// ============================================================================

std::unique_ptr<WipeIteratorHandle> FdbHandle::wipe(rust::Str request, bool doit, bool porcelain,
                                                    bool unsafe_wipe_all) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().wipe(tool_request, doit, porcelain, unsafe_wipe_all);
    return std::make_unique<WipeIteratorHandle>(std::move(it));
}

// ============================================================================
// Purge functions
// ============================================================================

std::unique_ptr<PurgeIteratorHandle> FdbHandle::purge(rust::Str request, bool doit, bool porcelain) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().purge(tool_request, doit, porcelain);
    return std::make_unique<PurgeIteratorHandle>(std::move(it));
}

// ============================================================================
// Stats functions
// ============================================================================

std::unique_ptr<StatsIteratorHandle> FdbHandle::stats_iterator(rust::Str request) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = inner().stats(tool_request);
    return std::make_unique<StatsIteratorHandle>(std::move(it));
}

// ============================================================================
// Control functions
// ============================================================================

std::unique_ptr<ControlIteratorHandle> FdbHandle::control(rust::Str request, fdb5::ControlAction action,
                                                          rust::Slice<const fdb5::ControlIdentifier> identifiers) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);

    fdb5::ControlIdentifiers ctrl_ids;
    for (auto id : identifiers) {
        ctrl_ids |= id;
    }

    auto it = inner().control(tool_request, action, ctrl_ids);
    return std::make_unique<ControlIteratorHandle>(std::move(it));
}

// ============================================================================
// Callback registration functions
// ============================================================================

void FdbHandle::register_flush_callback(rust::Box<FlushCallbackBox> callback) {
    // Create a shared_ptr to hold the callback box so it can be captured by the lambda
    auto callback_ptr = std::make_shared<rust::Box<FlushCallbackBox>>(std::move(callback));

    fdb5::FlushCallback cpp_callback = [callback_ptr]() { invoke_flush_callback(**callback_ptr); };

    inner().registerFlushCallback(std::move(cpp_callback));
}

void FdbHandle::register_archive_callback(rust::Box<ArchiveCallbackBox> callback) {
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

    inner().registerArchiveCallback(std::move(cpp_callback));
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
