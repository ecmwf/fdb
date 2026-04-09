// fdb_bridge.cpp - C++ bridge implementation
//
// This file implements the shim functions that convert between the native
// FDB5 C++ API and cxx-compatible types.

#include "fdb_bridge.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/fdb5_version.h"

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/runtime/Main.h"
#include "metkit/mars/MarsRequest.h"

#include <mutex>
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

/// Parse a key=value string (no verb) into a MarsRequest
static metkit::mars::MarsRequest parse_request_no_verb(const std::string& request_str) {
    if (request_str.empty()) {
        return metkit::mars::MarsRequest{};
    }

    // Create MarsRequest with empty verb
    metkit::mars::MarsRequest mars("");

    // Parse key=value pairs separated by commas
    // Format: key1=val1/val2,key2=val3,...
    std::string::size_type pos = 0;
    while (pos < request_str.size()) {
        // Find key
        auto eq_pos = request_str.find('=', pos);
        if (eq_pos == std::string::npos) {
            break;
        }
        std::string key = request_str.substr(pos, eq_pos - pos);

        // Find values (until comma or end)
        auto comma_pos = request_str.find(',', eq_pos);
        std::string values_str;
        if (comma_pos == std::string::npos) {
            values_str = request_str.substr(eq_pos + 1);
            pos = request_str.size();
        }
        else {
            values_str = request_str.substr(eq_pos + 1, comma_pos - eq_pos - 1);
            pos = comma_pos + 1;
        }

        // Split values by '/'
        std::vector<std::string> values;
        std::string::size_type vpos = 0;
        while (vpos < values_str.size()) {
            auto slash_pos = values_str.find('/', vpos);
            if (slash_pos == std::string::npos) {
                values.push_back(values_str.substr(vpos));
                break;
            }
            values.push_back(values_str.substr(vpos, slash_pos - vpos));
            vpos = slash_pos + 1;
        }

        mars.values(key, values);
    }

    return mars;
}

/// Create FDBToolRequest from request string
static fdb5::FDBToolRequest make_tool_request(const std::string& request_str) {
    auto mars = parse_request_no_verb(request_str);
    // If request is empty, match all; otherwise filter by request
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
// DataReaderHandle implementation
// ============================================================================

DataReaderHandle::DataReaderHandle(std::unique_ptr<eckit::DataHandle> handle) : impl_(std::move(handle)) {}

DataReaderHandle::~DataReaderHandle() {
    if (is_open_ && impl_) {
        try {
            impl_->close();
        }
        catch (const std::exception&) {
            // Destructors must not throw - swallow exception
        }
    }
}

void DataReaderHandle::open() {
    if (impl_ && !is_open_) {
        impl_->openForRead();
        is_open_ = true;
    }
}

void DataReaderHandle::close() {
    if (impl_ && is_open_) {
        impl_->close();
        is_open_ = false;
    }
}

size_t DataReaderHandle::read(rust::Slice<uint8_t> buffer) {
    if (!impl_ || !is_open_) {
        throw std::runtime_error("DataReader not open");
    }
    return impl_->read(buffer.data(), buffer.size());
}

void DataReaderHandle::seek(uint64_t position) {
    if (!impl_ || !is_open_) {
        throw std::runtime_error("DataReader not open");
    }
    impl_->seek(eckit::Offset(position));
}

uint64_t DataReaderHandle::tell() const {
    if (!impl_) {
        return 0;
    }
    return impl_->position();
}

uint64_t DataReaderHandle::size() const {
    if (!impl_) {
        return 0;
    }
    return impl_->size();
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
        throw std::runtime_error("Iterator exhausted");
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
        throw std::runtime_error("Iterator exhausted");
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
        throw std::runtime_error("Iterator exhausted");
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
        throw std::runtime_error("Iterator exhausted");
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
        throw std::runtime_error("Iterator exhausted");
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
        throw std::runtime_error("Iterator exhausted");
    }

    has_current_ = false;

    StatsElementData data;
    // StatsElement is a DbStats - access via indexStatistics methods
    data.location = rust::String("");
    data.field_count = current_.indexStatistics.fieldsCount();
    data.total_size = current_.indexStatistics.fieldsSize();
    data.duplicate_count = current_.indexStatistics.duplicatesCount();
    data.duplicate_size = current_.indexStatistics.duplicatesSize();
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
        throw std::runtime_error("Iterator exhausted");
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
// MoveIteratorHandle implementation
// ============================================================================

MoveIteratorHandle::MoveIteratorHandle(fdb5::MoveIterator&& it) : impl_(std::move(it)) {}

MoveIteratorHandle::~MoveIteratorHandle() = default;

bool MoveIteratorHandle::hasNext() {
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

MoveElementData MoveIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw std::runtime_error("Iterator exhausted");
    }

    has_current_ = false;

    MoveElementData data;
    // MoveElement is FileCopy - convert to string representation
    std::ostringstream ss;
    ss << current_;
    data.source = rust::String(ss.str());
    data.destination = rust::String("");
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

std::unique_ptr<FdbHandle> new_fdb_from_yaml(rust::Str config) {
    return std::make_unique<FdbHandle>(std::string(config));
}

std::unique_ptr<FdbHandle> new_fdb_from_yaml_with_user_config(rust::Str config, rust::Str user_config) {
    return std::make_unique<FdbHandle>(std::string(config), std::string(user_config));
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

// ============================================================================
// Retrieve functions
// ============================================================================

std::unique_ptr<DataReaderHandle> retrieve(FdbHandle& handle, rust::Str request) {
    std::string request_str{request};
    auto mars = parse_request_no_verb(request_str);
    eckit::DataHandle* dh = handle.inner().retrieve(mars);
    return std::make_unique<DataReaderHandle>(std::unique_ptr<eckit::DataHandle>(dh));
}

// ============================================================================
// Read functions (by URI)
// ============================================================================

std::unique_ptr<DataReaderHandle> read_uri(FdbHandle& handle, rust::Str uri) {
    std::string uri_str{uri};
    eckit::URI eckit_uri{uri_str};
    eckit::DataHandle* dh = handle.inner().read(eckit_uri);
    return std::make_unique<DataReaderHandle>(std::unique_ptr<eckit::DataHandle>(dh));
}

std::unique_ptr<DataReaderHandle> read_uris(FdbHandle& handle, const rust::Vec<rust::String>& uris,
                                            bool in_storage_order) {
    std::vector<eckit::URI> eckit_uris;
    eckit_uris.reserve(uris.size());
    for (const auto& uri : uris) {
        eckit_uris.emplace_back(std::string(uri));
    }
    eckit::DataHandle* dh = handle.inner().read(eckit_uris, in_storage_order);
    return std::make_unique<DataReaderHandle>(std::unique_ptr<eckit::DataHandle>(dh));
}

std::unique_ptr<DataReaderHandle> read_list_iterator(FdbHandle& handle, ListIteratorHandle& iterator,
                                                     bool in_storage_order) {
    // Calls FDB::read(ListIterator&, bool) directly - most efficient path
    eckit::DataHandle* dh = handle.inner().read(iterator.inner(), in_storage_order);
    return std::make_unique<DataReaderHandle>(std::unique_ptr<eckit::DataHandle>(dh));
}

// ============================================================================
// List functions
// ============================================================================

std::unique_ptr<ListIteratorHandle> list(FdbHandle& handle, rust::Str request, bool deduplicate, int32_t level) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().list(tool_request, deduplicate, level);
    return std::make_unique<ListIteratorHandle>(std::move(it));
}

// ============================================================================
// Axes query functions
// ============================================================================

rust::Vec<AxisEntry> axes(FdbHandle& handle, rust::Str request, int32_t level) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
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

std::unique_ptr<DumpIteratorHandle> dump(FdbHandle& handle, rust::Str request, bool simple) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().dump(tool_request, simple);
    return std::make_unique<DumpIteratorHandle>(std::move(it));
}

// ============================================================================
// Status functions
// ============================================================================

std::unique_ptr<StatusIteratorHandle> status(FdbHandle& handle, rust::Str request) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().status(tool_request);
    return std::make_unique<StatusIteratorHandle>(std::move(it));
}

// ============================================================================
// Wipe functions
// ============================================================================

std::unique_ptr<WipeIteratorHandle> wipe(FdbHandle& handle, rust::Str request, bool doit, bool porcelain,
                                         bool unsafe_wipe_all) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().wipe(tool_request, doit, porcelain, unsafe_wipe_all);
    return std::make_unique<WipeIteratorHandle>(std::move(it));
}

// ============================================================================
// Purge functions
// ============================================================================

std::unique_ptr<PurgeIteratorHandle> purge(FdbHandle& handle, rust::Str request, bool doit, bool porcelain) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().purge(tool_request, doit, porcelain);
    return std::make_unique<PurgeIteratorHandle>(std::move(it));
}

// ============================================================================
// Stats functions
// ============================================================================

std::unique_ptr<StatsIteratorHandle> stats_iterator(FdbHandle& handle, rust::Str request) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);
    auto it = handle.inner().stats(tool_request);
    return std::make_unique<StatsIteratorHandle>(std::move(it));
}

// ============================================================================
// Control functions
// ============================================================================

std::unique_ptr<ControlIteratorHandle> control(FdbHandle& handle, rust::Str request, fdb5::ControlAction action,
                                               rust::Slice<const fdb5::ControlIdentifier> identifiers) {
    std::string request_str{request};
    auto tool_request = make_tool_request(request_str);

    fdb5::ControlIdentifiers ctrl_ids;
    for (auto id : identifiers) {
        ctrl_ids |= id;
    }

    auto it = handle.inner().control(tool_request, action, ctrl_ids);
    return std::make_unique<ControlIteratorHandle>(std::move(it));
}

// ============================================================================
// Move functions
// ============================================================================

std::unique_ptr<MoveIteratorHandle> move_data(FdbHandle& handle, rust::Str request, rust::Str dest) {
    std::string request_str{request};
    std::string dest_str{dest};
    auto tool_request = make_tool_request(request_str);
    eckit::URI dest_uri{dest_str};
    auto it = handle.inner().move(tool_request, dest_uri);
    return std::make_unique<MoveIteratorHandle>(std::move(it));
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
