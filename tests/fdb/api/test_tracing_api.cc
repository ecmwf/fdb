// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/fdb_c.h"
#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

namespace fdb5::test {

using namespace fdb5::tracing;

namespace {

constexpr const char* callerTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

class CollectingExporter : public SpanExporter {
public:

    void exportSpan(const SpanData& span) override {
        std::lock_guard lock(mutex_);
        spans_.push_back(span);
    }

    std::vector<SpanData> spans() const {
        std::lock_guard lock(mutex_);
        return spans_;
    }

    std::vector<SpanData> named(const std::string& name) const {
        std::vector<SpanData> out;
        for (const auto& s : spans()) {
            if (s.name == name) {
                out.push_back(s);
            }
        }
        return out;
    }

    void clear() {
        std::lock_guard lock(mutex_);
        spans_.clear();
    }

private:

    mutable std::mutex mutex_;
    std::vector<SpanData> spans_;
};

struct Fixture {
    Fixture() :
        tmpdir(eckit::LocalPathName::cwd().c_str()),
        env{"FDB_ROOT_DIRECTORY", tmpdir.asString().c_str()},
        exporter(std::make_shared<CollectingExporter>()) {
        Tracer::instance().exporter(exporter);
        Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOn, true});
    }
    ~Fixture() { Tracer::instance().exporter(nullptr); }

    eckit::TmpDir tmpdir;
    eckit::testing::SetEnv env;
    std::shared_ptr<CollectingExporter> exporter;
};

Key makeKey(const std::string& step) {
    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("type", "fc");
    key.set("stream", "oper");
    key.set("date", "20101010");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("param", "130");
    key.set("step", step);
    return key;
}

const std::string payload = "Raining cats and dogs";

void archiveSome(FDB& fdb, size_t n, const TraceContext& trace = {}) {
    for (size_t i = 0; i < n; ++i) {
        fdb.archive(makeKey(std::to_string(i)), payload.data(), payload.size(), trace);
    }
}

/// An integer attribute of a span, or -1 if it has none
int64_t attribute(const SpanData& span, const std::string& name) {
    for (const auto& [key, value] : span.attributes) {
        if (key == name) {
            return std::get<int64_t>(value);
        }
    }
    return -1;
}

bool hasAttribute(const SpanData& span, const std::string& name) {
    for (const auto& a : span.attributes) {
        if (a.first == name) {
            return true;
        }
    }
    return false;
}

FDBToolRequest allRequest() {
    return FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Archive without a caller context joins the archive-cycle trace, closed by flush") {
    Fixture fx;
    FDB fdb;

    archiveSome(fdb, 3);
    EXPECT(fx.exporter->named("fdb.archive_cycle").empty());  // cycle still open

    fdb.flush();

    auto cycle = fx.exporter->named("fdb.archive_cycle");
    auto archives = fx.exporter->named("fdb.archive");
    auto flushes = fx.exporter->named("fdb.flush");
    EXPECT_EQUAL(cycle.size(), 1);
    EXPECT_EQUAL(archives.size(), 1);  // the archives of the cycle are aggregated
    EXPECT_EQUAL(flushes.size(), 1);

    const auto& traceId = cycle[0].context.traceId();
    EXPECT(TraceContext(traceId, cycle[0].parentSpanId, 0).spanIdHex() == "0000000000000000");  // root
    EXPECT(archives[0].context.traceId() == traceId);
    EXPECT(archives[0].parentSpanId == cycle[0].context.spanId());
    EXPECT_EQUAL(attribute(archives[0], "fdb.calls"), 3);
    EXPECT_EQUAL(attribute(archives[0], "fdb.fields"), 3);
    EXPECT_EQUAL(attribute(archives[0], "fdb.bytes"), static_cast<int64_t>(3 * payload.size()));
    EXPECT(attribute(archives[0], "fdb.busy_ns") > 0);
    EXPECT(archives[0].end <= flushes[0].start);  // ends with the last archive, before the flush
    EXPECT(flushes[0].parentSpanId == cycle[0].context.spanId());
    EXPECT(flushes[0].end <= cycle[0].end);

    // the next cycle is a new trace
    fx.exporter->clear();
    archiveSome(fdb, 1);
    fdb.flush();
    auto cycle2 = fx.exporter->named("fdb.archive_cycle");
    EXPECT_EQUAL(cycle2.size(), 1);
    EXPECT(cycle2[0].context.traceId() != traceId);
}

CASE("An explicit caller context is the parent of the API span") {
    Fixture fx;
    FDB fdb;
    TraceContext caller = TraceContext::fromTraceparent(callerTraceparent);

    archiveSome(fdb, 2, caller);
    fdb.flush(caller);

    EXPECT(fx.exporter->named("fdb.archive_cycle").empty());
    auto archives = fx.exporter->named("fdb.archive");
    EXPECT_EQUAL(archives.size(), 1);
    EXPECT(archives[0].context.traceId() == caller.traceId());
    EXPECT(archives[0].parentSpanId == caller.spanId());
    EXPECT_EQUAL(attribute(archives[0], "fdb.fields"), 2);
    auto flushes = fx.exporter->named("fdb.flush");
    EXPECT_EQUAL(flushes.size(), 1);
    EXPECT(flushes[0].parentSpanId == caller.spanId());
}

CASE("The calling thread's current context is used when no explicit context is given") {
    Fixture fx;
    FDB fdb;
    TraceContext caller = TraceContext::fromTraceparent(callerTraceparent);
    {
        ScopedContext scope(caller);
        archiveSome(fdb, 1);
        fdb.flush();
    }
    EXPECT(fx.exporter->named("fdb.archive_cycle").empty());
    auto archives = fx.exporter->named("fdb.archive");
    EXPECT_EQUAL(archives.size(), 1);
    EXPECT(archives[0].parentSpanId == caller.spanId());
}

CASE("List span ends when the iteration is exhausted") {
    Fixture fx;
    FDB fdb;
    archiveSome(fdb, 3);
    fdb.flush();
    fx.exporter->clear();

    TraceContext caller = TraceContext::fromTraceparent(callerTraceparent);
    ListIterator it = fdb.list(allRequest(), ListMode::Deduplicate, 3, caller);
    ListElement elem;
    size_t count = 0;
    while (it.next(elem)) {
        EXPECT(fx.exporter->named("fdb.list").empty());
        ++count;
    }
    EXPECT_EQUAL(count, 3);
    auto lists = fx.exporter->named("fdb.list");
    EXPECT_EQUAL(lists.size(), 1);
    EXPECT(lists[0].parentSpanId == caller.spanId());
}

CASE("Retrieve records inspect and read as child spans") {
    Fixture fx;
    FDB fdb;
    archiveSome(fdb, 2);
    fdb.flush();
    fx.exporter->clear();

    metkit::mars::MarsRequest request = makeKey("0").request("retrieve");
    request.values("step", {"0", "1"});

    std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(request));
    EXPECT(dh);

    auto retrieves = fx.exporter->named("fdb.retrieve");
    auto inspects = fx.exporter->named("fdb.inspect");
    auto reads = fx.exporter->named("fdb.read");
    EXPECT_EQUAL(retrieves.size(), 1);
    EXPECT_EQUAL(inspects.size(), 1);
    EXPECT_EQUAL(reads.size(), 1);
    EXPECT(inspects[0].parentSpanId == retrieves[0].context.spanId());
    EXPECT(reads[0].parentSpanId == retrieves[0].context.spanId());
    EXPECT(reads[0].context.traceId() == retrieves[0].context.traceId());
}

CASE("Purge and wipe are traced") {
    Fixture fx;
    FDB fdb;
    archiveSome(fdb, 2);
    fdb.flush();
    fx.exporter->clear();

    TraceContext caller = TraceContext::fromTraceparent(callerTraceparent);
    {
        PurgeIterator it = fdb.purge(allRequest(), false, false, caller);
        PurgeElement elem;
        while (it.next(elem)) {}
    }
    {
        WipeIterator it = fdb.wipe(allRequest(), false, false, false, caller);
        WipeElement elem;
        while (it.next(elem)) {}
    }
    auto purges = fx.exporter->named("fdb.purge");
    auto wipes = fx.exporter->named("fdb.wipe");
    EXPECT_EQUAL(purges.size(), 1);
    EXPECT_EQUAL(wipes.size(), 1);
    EXPECT(purges[0].parentSpanId == caller.spanId());
    EXPECT(wipes[0].parentSpanId == caller.spanId());
}

CASE("Failures: unsampled archive cycles are exported only if an archive fails") {
    Fixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    FDB fdb;

    archiveSome(fdb, 2);
    fdb.flush();
    EXPECT(fx.exporter->spans().empty());

    archiveSome(fdb, 2);
    Key invalid = makeKey("0");
    invalid.unset("param");  // matches no rule of the schema
    EXPECT_THROWS(fdb.archive(invalid, payload.data(), payload.size()));
    EXPECT_EQUAL(fx.exporter->named("fdb.archive").size(), 1);  // exported at once, not at the flush
    archiveSome(fdb, 1);
    fdb.flush();

    auto archives = fx.exporter->named("fdb.archive");
    auto cycle = fx.exporter->named("fdb.archive_cycle");
    EXPECT_EQUAL(archives.size(), 2);  // ended by the failure, and the next one
    EXPECT_EQUAL(cycle.size(), 1);
    EXPECT_EQUAL(fx.exporter->named("fdb.flush").size(), 1);

    // the failing call ends its aggregate, with the key that failed
    EXPECT(archives[0].status == StatusCode::Error);
    EXPECT_EQUAL(attribute(archives[0], "fdb.calls"), 3);
    EXPECT_EQUAL(attribute(archives[0], "fdb.fields"), 2);
    EXPECT(hasAttribute(archives[0], "fdb.key"));
    EXPECT(archives[1].status == StatusCode::Unset);
    EXPECT_EQUAL(attribute(archives[1], "fdb.fields"), 1);
    for (const auto& a : archives) {
        EXPECT(a.parentSpanId == cycle[0].context.spanId());
        EXPECT(!a.context.sampled());
    }
    Tracer::instance().sampler(Sampler{});
}

CASE("Failures: a failing flush exports the archive spans of its trace") {
    Fixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    {
        FDB fdb;
        bool fail = true;
        fdb.registerFlushCallback([&fail] {
            if (std::exchange(fail, false)) {
                throw eckit::SeriousBug("flush failed", Here());
            }
        });

        archiveSome(fdb, 2, unsampled);
        EXPECT_THROWS_AS(fdb.flush(unsampled), eckit::SeriousBug);

        auto flushes = fx.exporter->named("fdb.flush");
        auto archives = fx.exporter->named("fdb.archive");
        EXPECT_EQUAL(flushes.size(), 1);
        EXPECT(flushes[0].status == StatusCode::Error);
        EXPECT_EQUAL(archives.size(), 1);
        EXPECT_EQUAL(attribute(archives[0], "fdb.fields"), 2);
        EXPECT(archives[0].parentSpanId == unsampled.spanId());
        EXPECT(archives[0].end <= flushes[0].start);

        fx.exporter->clear();
        fdb.flush(unsampled);  // succeeds: nothing exported
        EXPECT(fx.exporter->spans().empty());
    }
    Tracer::instance().sampler(Sampler{});
}

CASE("Failures: a failing flush exports the archive spans of other traces") {
    // The archives have an explicit (unsampled) context, the flush has none: it is in another trace, which must not
    // be the only one to fail
    Fixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    {
        FDB fdb;
        fdb.registerFlushCallback([] { throw eckit::SeriousBug("flush failed", Here()); });

        archiveSome(fdb, 2, unsampled);
        EXPECT_THROWS_AS(fdb.flush(), eckit::SeriousBug);

        auto flushes = fx.exporter->named("fdb.flush");
        auto archives = fx.exporter->named("fdb.archive");
        EXPECT_EQUAL(flushes.size(), 1);
        EXPECT(flushes[0].status == StatusCode::Error);
        EXPECT(flushes[0].context.traceId() != unsampled.traceId());
        EXPECT_EQUAL(archives.size(), 1);
        EXPECT(archives[0].status == StatusCode::Unset);  // the archives succeeded: only the flush failed
        EXPECT(archives[0].context.traceId() == unsampled.traceId());
        EXPECT_EQUAL(attribute(archives[0], "fdb.fields"), 2);
    }

    Tracer::instance().sampler(Sampler{});
}

CASE("Failures: a failing archive exports the traces of the archives not yet flushed") {
    // The backend may fail to flush earlier data during an archive (e.g. that of a database evicted by the Archiver):
    // the traces of the archives whose data is not yet durable are part of the failure, whichever trace they belong to
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    // a failure in the backend, and an invalid request (missing messages), in the archive-cycle trace
    const std::vector<std::function<void(FDB&)>> failures{
        [](FDB& fdb) {
            Key invalid = makeKey("0");
            invalid.unset("param");  // matches no rule of the schema
            fdb.archive(invalid, payload.data(), payload.size());
        },
        [](FDB& fdb) {
            eckit::MemoryHandle empty(payload.data(), 0);
            fdb.archive(makeKey("9").request("archive"), empty);
        }};

    for (const auto& fail : failures) {
        Fixture fx;
        Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
        FDB fdb;
        archiveSome(fdb, 1, unsampled);
        fdb.flush(unsampled);  // durable: not part of the failure
        EXPECT(fx.exporter->spans().empty());

        archiveSome(fdb, 2, unsampled);
        EXPECT_THROWS(fail(fdb));
        fdb.flush();  // succeeds

        size_t retained = 0;
        for (const auto& a : fx.exporter->named("fdb.archive")) {
            if (a.context.traceId() == unsampled.traceId()) {
                ++retained;
                EXPECT(a.status == StatusCode::Unset);  // the archives themselves succeeded
                EXPECT_EQUAL(attribute(a, "fdb.fields"), 2);
            }
        }
        EXPECT_EQUAL(retained, 1);
    }
    Tracer::instance().sampler(Sampler{});
}

CASE("A concurrent flush never closes the cycle that an archive is joining") {
    // The archive cycle is closed by a flush either before an archive chooses its parent (the archive is in the next
    // cycle), or after it is registered in its aggregate: an aggregate never starts in a cycle that has ended (its
    // trace would be complete, and its spans not exported if the next flush fails)
    Fixture fx;
    constexpr size_t n = 2000;

    {
        FDB fdb;
        std::atomic<bool> done{false};
        std::thread flusher([&] {
            while (!done) {
                fdb.flush();
            }
        });
        archiveSome(fdb, n);
        done = true;
        flusher.join();
        fdb.flush();
    }

    std::map<TraceContext::SpanId, SpanData> cycles;
    for (const auto& cycle : fx.exporter->named("fdb.archive_cycle")) {
        cycles.emplace(cycle.context.spanId(), cycle);
    }
    size_t fields = 0;
    for (const auto& archive : fx.exporter->named("fdb.archive")) {
        auto cycle = cycles.find(archive.parentSpanId);
        EXPECT(cycle != cycles.end());
        EXPECT(archive.start <= cycle->second.end);
        fields += attribute(archive, "fdb.fields");
    }
    EXPECT_EQUAL(fields, n);
}

CASE("A moved-from FDB can be destroyed") {
    Fixture fx;
    auto fdb = std::make_unique<FDB>();
    archiveSome(*fdb, 1);
    FDB other(std::move(*fdb));
    fdb.reset();
    other.flush();
    EXPECT_EQUAL(fx.exporter->named("fdb.archive").size(), 1);
}

CASE("Tracing disabled in the process records nothing") {
    Fixture fx;
    struct Disabled {
        Disabled() { configureEnabled(false); }
        ~Disabled() { configureEnabled(true); }
        static void configureEnabled(bool enabled) {
            eckit::LocalConfiguration config;
            config.set("enabled", enabled);
            Tracer::instance().configure(config);
        }
    } disabled;

    FDB fdb;
    archiveSome(fdb, 2, TraceContext::fromTraceparent(callerTraceparent));
    archiveSome(fdb, 2);
    fdb.flush();
    ListIterator it = fdb.list(allRequest());
    ListElement elem;
    EXPECT(it.next(elem));
    EXPECT(fx.exporter->spans().empty());
}

namespace {

/// An iteration whose implementation records a failing span when it is destroyed (e.g. a cancelled worker)
class FailingOnDestruction : public APIIteratorBase<int> {
public:

    explicit FailingOnDestruction(TraceContext parent) : parent_(std::move(parent)) {}

    ~FailingOnDestruction() override {
        Span cleanup("cleanup", SpanKind::Internal, parent_);
        cleanup.setStatus(StatusCode::Error, "cleanup failed");
    }

    bool next(int&) override { return true; }

private:

    TraceContext parent_;
};

}  // namespace

CASE("Failures: the span of an abandoned iteration ends after its implementation") {
    Fixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, true});

    // An iteration in an unsampled trace, whose implementation fails when it is destroyed
    auto failingIteration = [] {
        const TraceContext unsampled{Tracer::newTraceId(), Tracer::newSpanId(), 0};
        Span span("fdb.list", SpanKind::Internal, unsampled);
        APIIterator<int> it(new FailingOnDestruction(span.context()));
        it.attachSpan(std::move(span));
        int i = 0;
        EXPECT(it.next(i));
        return it;
    };

    // the iteration span is still open when its implementation fails: its trace is exported
    SECTION("destroyed") {
        { APIIterator<int> it = failingIteration(); }
        EXPECT_EQUAL(fx.exporter->named("cleanup").size(), 1);
        EXPECT_EQUAL(fx.exporter->named("fdb.list").size(), 1);
    }

    SECTION("replaced by move assignment") {
        APIIterator<int> it = failingIteration();
        it = APIIterator<int>(nullptr);
        EXPECT_EQUAL(fx.exporter->named("cleanup").size(), 1);
        EXPECT_EQUAL(fx.exporter->named("fdb.list").size(), 1);
    }
    Tracer::instance().sampler(Sampler{});
}

CASE("C API: fdb_trace_begin/end scope the calling thread's context") {
    EXPECT(!current().valid());
    EXPECT_EQUAL(fdb_trace_begin(callerTraceparent, "vendor=abc"), FDB_SUCCESS);
    EXPECT_EQUAL(current().traceparent(), std::string{callerTraceparent});
    EXPECT_EQUAL(current().traceState(), "vendor=abc");

    const char* nested = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    EXPECT_EQUAL(fdb_trace_begin(nested, nullptr), FDB_SUCCESS);
    EXPECT_EQUAL(current().traceparent(), std::string{nested});
    EXPECT_EQUAL(fdb_trace_end(), FDB_SUCCESS);
    EXPECT_EQUAL(current().traceparent(), std::string{callerTraceparent});

    EXPECT_EQUAL(fdb_trace_end(), FDB_SUCCESS);
    EXPECT(!current().valid());

    EXPECT(fdb_trace_end() != FDB_SUCCESS);  // unbalanced
    EXPECT(fdb_trace_begin("garbage", nullptr) != FDB_SUCCESS);
    EXPECT(!current().valid());
}

CASE("C API: archives within a trace scope are children of the caller's context") {
    Fixture fx;
    fdb_handle_t* fdb = nullptr;
    EXPECT_EQUAL(fdb_new_handle(&fdb), FDB_SUCCESS);

    fdb_key_t* key = nullptr;
    EXPECT_EQUAL(fdb_new_key(&key), FDB_SUCCESS);
    Key k = makeKey("7");
    for (const auto& [name, value] : k) {
        EXPECT_EQUAL(fdb_key_add(key, name.c_str(), value.c_str()), FDB_SUCCESS);
    }

    EXPECT_EQUAL(fdb_trace_begin(callerTraceparent, nullptr), FDB_SUCCESS);
    EXPECT_EQUAL(fdb_archive(fdb, key, payload.data(), payload.size()), FDB_SUCCESS);
    EXPECT_EQUAL(fdb_flush(fdb), FDB_SUCCESS);
    EXPECT_EQUAL(fdb_trace_end(), FDB_SUCCESS);

    EXPECT_EQUAL(fdb_delete_key(key), FDB_SUCCESS);
    EXPECT_EQUAL(fdb_delete_handle(fdb), FDB_SUCCESS);

    TraceContext caller = TraceContext::fromTraceparent(callerTraceparent);
    auto archives = fx.exporter->named("fdb.archive");
    EXPECT_EQUAL(archives.size(), 1);
    EXPECT(archives[0].parentSpanId == caller.spanId());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
