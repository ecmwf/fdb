// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/fdb_c.h"
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
    EXPECT_EQUAL(archives.size(), 3);
    EXPECT_EQUAL(flushes.size(), 1);

    const auto& traceId = cycle[0].context.traceId();
    EXPECT(TraceContext(traceId, cycle[0].parentSpanId, 0).spanIdHex() == "0000000000000000");  // root
    for (const auto& a : archives) {
        EXPECT(a.context.traceId() == traceId);
        EXPECT(a.parentSpanId == cycle[0].context.spanId());
    }
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
    EXPECT_EQUAL(archives.size(), 2);
    for (const auto& a : archives) {
        EXPECT(a.context.traceId() == caller.traceId());
        EXPECT(a.parentSpanId == caller.spanId());
        EXPECT_EQUAL(a.attributes.size(), 3);  // fdb.key, fdb.fields, fdb.bytes
    }
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

CASE("keep_errors: unsampled archive cycles are exported only if an archive fails") {
    Fixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    Tracer::instance().keepErrors(true);
    FDB fdb;

    archiveSome(fdb, 2);
    fdb.flush();
    EXPECT(fx.exporter->spans().empty());

    archiveSome(fdb, 2);
    Key invalid = makeKey("0");
    invalid.unset("param");  // matches no rule of the schema
    EXPECT_THROWS(fdb.archive(invalid, payload.data(), payload.size()));
    archiveSome(fdb, 1);
    fdb.flush();

    auto archives = fx.exporter->named("fdb.archive");
    auto cycle = fx.exporter->named("fdb.archive_cycle");
    EXPECT_EQUAL(archives.size(), 4);  // those held before the failure, the failure, and the next one
    EXPECT_EQUAL(cycle.size(), 1);
    EXPECT_EQUAL(fx.exporter->named("fdb.flush").size(), 1);
    EXPECT_EQUAL(std::count_if(archives.begin(), archives.end(),
                               [](const SpanData& s) { return s.status == StatusCode::Error; }),
                 1);
    for (const auto& a : archives) {
        EXPECT(a.parentSpanId == cycle[0].context.spanId());
        EXPECT(!a.context.sampled());
    }

    Tracer::instance().keepErrors(false);
    Tracer::instance().sampler(Sampler{});
}

CASE("Tracing disabled in the FDB configuration records nothing") {
    Fixture fx;
    Config config = Config().expandConfig();
    config.set("tracing", false);
    FDB fdb(config);

    archiveSome(fdb, 2, TraceContext::fromTraceparent(callerTraceparent));
    fdb.flush();
    EXPECT(fx.exporter->spans().empty());
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
