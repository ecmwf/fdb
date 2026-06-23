// fdb FDB bridge — implementation.

#include "fdb_exceptions.h"

#include "ControlIteratorHandle.h"
#include "DumpIteratorHandle.h"
#include "FdbHandle.h"
#include "Key.h"
#include "ListIteratorHandle.h"
#include "PurgeIteratorHandle.h"
#include "StatsIteratorHandle.h"
#include "StatusIteratorHandle.h"
#include "WipeIteratorHandle.h"

#include "fdb-sys/src/lib.rs.h"
#include "metkit-sys/src/lib.rs.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/fdb5_version.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "eckit/runtime/Main.h"
#include "metkit/mars/MarsRequest.h"

#include <future>
#include <memory>
#include <mutex>
#include <vector>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

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

//----------------------------------------------------------------------------------------------------------------------

void FdbHandle::archive(const KeyData& key, rust::Slice<const uint8_t> data) {
    fdb5::Key fdb_key = Key::from_data(key);
    impl_.archive(fdb_key, data.data(), data.size());
}

void FdbHandle::archive_raw(rust::Slice<const uint8_t> data) {
    impl_.archive(data.data(), data.size());
}

namespace {

/// `eckit::DataHandle` adapter that pulls bytes from a Rust `std::io::Read`
/// source via the cxx callback `invoke_reader_read`. Used by
/// `FdbHandle::archive_reader` to stream Rust-side data into
/// `fdb5::FDB::archive(eckit::DataHandle&)` without buffering the whole
/// payload in memory first.
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
    impl_.archive(adapter);
}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<eckit_bridge::DataHandleWrapper> FdbHandle::retrieve(const metkit_bridge::MarsRequestWrapper& request) {
    return std::make_unique<eckit_bridge::DataHandleWrapper>(impl_.retrieve(request.inner()));
}

std::unique_ptr<eckit_bridge::DataHandleWrapper> FdbHandle::read_uri(rust::Str uri) {
    std::string uri_str{uri};
    eckit::URI eckit_uri{uri_str};
    return std::make_unique<eckit_bridge::DataHandleWrapper>(impl_.read(eckit_uri));
}

std::unique_ptr<eckit_bridge::DataHandleWrapper> FdbHandle::read_uris(const rust::Vec<rust::String>& uris,
                                                                      bool in_storage_order) {
    std::vector<eckit::URI> eckit_uris;
    eckit_uris.reserve(uris.size());
    for (const auto& uri : uris) {
        eckit_uris.emplace_back(std::string(uri));
    }
    return std::make_unique<eckit_bridge::DataHandleWrapper>(impl_.read(eckit_uris, in_storage_order));
}

std::unique_ptr<eckit_bridge::DataHandleWrapper> FdbHandle::read_list_iterator(ListIteratorHandle& iterator,
                                                                               bool in_storage_order) {
    return std::make_unique<eckit_bridge::DataHandleWrapper>(impl_.read(iterator.inner(), in_storage_order));
}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<ListIteratorHandle> FdbHandle::list(const metkit_bridge::MarsRequestWrapper& request, bool deduplicate,
                                                    int32_t level) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.list(tool_request, deduplicate, level);
    return std::make_unique<ListIteratorHandle>(std::move(it));
}

rust::Vec<AxisEntry> FdbHandle::axes(const metkit_bridge::MarsRequestWrapper& request, int32_t level) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto index_axis = impl_.axes(tool_request, level);

    rust::Vec<AxisEntry> result;
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

std::unique_ptr<DumpIteratorHandle> FdbHandle::dump(const metkit_bridge::MarsRequestWrapper& request, bool simple) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.dump(tool_request, simple);
    return std::make_unique<DumpIteratorHandle>(std::move(it));
}

std::unique_ptr<StatusIteratorHandle> FdbHandle::status(const metkit_bridge::MarsRequestWrapper& request) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.status(tool_request);
    return std::make_unique<StatusIteratorHandle>(std::move(it));
}

std::unique_ptr<WipeIteratorHandle> FdbHandle::wipe(const metkit_bridge::MarsRequestWrapper& request, bool doit,
                                                    bool porcelain, bool unsafe_wipe_all) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.wipe(tool_request, doit, porcelain, unsafe_wipe_all);
    return std::make_unique<WipeIteratorHandle>(std::move(it));
}

std::unique_ptr<PurgeIteratorHandle> FdbHandle::purge(const metkit_bridge::MarsRequestWrapper& request, bool doit,
                                                      bool porcelain) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.purge(tool_request, doit, porcelain);
    return std::make_unique<PurgeIteratorHandle>(std::move(it));
}

std::unique_ptr<StatsIteratorHandle> FdbHandle::stats_iterator(const metkit_bridge::MarsRequestWrapper& request) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};
    auto it = impl_.stats(tool_request);
    return std::make_unique<StatsIteratorHandle>(std::move(it));
}

std::unique_ptr<ControlIteratorHandle> FdbHandle::control(const metkit_bridge::MarsRequestWrapper& request,
                                                          fdb5::ControlAction action,
                                                          rust::Slice<const fdb5::ControlIdentifier> identifiers) {
    const auto& mars = request.inner();
    auto tool_request = fdb5::FDBToolRequest{mars, mars.empty(), std::vector<std::string>{}};

    fdb5::ControlIdentifiers ctrl_ids;
    for (auto id : identifiers) {
        ctrl_ids |= id;
    }

    auto it = impl_.control(tool_request, action, ctrl_ids);
    return std::make_unique<ControlIteratorHandle>(std::move(it));
}

//----------------------------------------------------------------------------------------------------------------------

void FdbHandle::register_flush_callback(rust::Box<FlushCallbackBox> callback) {
    auto callback_ptr = std::make_shared<rust::Box<FlushCallbackBox>>(std::move(callback));

    fdb5::FlushCallback cpp_callback = [callback_ptr]() { invoke_flush_callback(**callback_ptr); };

    impl_.registerFlushCallback(std::move(cpp_callback));
}

void FdbHandle::register_archive_callback(rust::Box<ArchiveCallbackBox> callback) {
    auto callback_ptr = std::make_shared<rust::Box<ArchiveCallbackBox>>(std::move(callback));

    fdb5::ArchiveCallback cpp_callback = [callback_ptr](
                                             const fdb5::Key& key, const void* data, size_t length,
                                             std::future<std::shared_ptr<const fdb5::FieldLocation>> location_future) {
        rust::Vec<KeyValue> key_vec = Key::to_data(key);

        rust::Slice<const uint8_t> data_slice{static_cast<const uint8_t*>(data), length};

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
            // best-effort — leave location info empty on future failure
        }

        rust::Slice<const KeyValue> key_slice{key_vec.data(), key_vec.size()};

        invoke_archive_callback(**callback_ptr, key_slice, data_slice, rust::Str(location_uri), location_offset,
                                location_length);
    };

    impl_.registerArchiveCallback(std::move(cpp_callback));
}

//----------------------------------------------------------------------------------------------------------------------

namespace {

std::once_flag g_init_flag;

}  // namespace

void FdbHandle::initialise() {
    std::call_once(g_init_flag, []() {
        if (!eckit::Main::ready()) {
            static const char* argv[] = {"fdb-sys", nullptr};
            eckit::Main::initialise(1, const_cast<char**>(argv));
        }
    });
}

rust::String FdbHandle::version() {
    return rust::String(fdb5_version_str());
}

rust::String FdbHandle::git_sha1() {
    return rust::String(fdb5_git_sha1());
}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<FdbHandle> FdbHandle::create() {
    return std::make_unique<FdbHandle>();
}

std::unique_ptr<FdbHandle> FdbHandle::from_config(const eckit_bridge::ConfigWrapper& config) {
    return std::make_unique<FdbHandle>(config);
}

std::unique_ptr<FdbHandle> FdbHandle::from_config_with_user(const eckit_bridge::ConfigWrapper& config,
                                                            const eckit_bridge::ConfigWrapper& user_config) {
    return std::make_unique<FdbHandle>(config, user_config);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
