// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// End-to-end trace-context propagation between a client and remote catalogue/store servers.
//
// Run by fdb_test_server.sh against servers exporting their spans to JSON-lines files (see tracing_*.yaml.in).
// FDB_TRACING_TEST_VARIANT selects the expectation:
//   enabled          - server spans join the caller's trace, as children of the client spans
//   client_disabled  - client has 'tracing: false': no client spans, no propagation (servers trace on their own)
//   server_disabled  - servers have tracing disabled: client spans only, servers record nothing (nor do their FDBs)

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/testing/Test.h"
#include "eckit/value/Value.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

namespace fdb5::test {

using namespace fdb5::tracing;

namespace {

struct ExportedSpan {
    std::string name;
    std::string traceId;
    std::string spanId;
    std::string parentSpanId;
    int64_t fields = 0;  // the fdb.fields attribute, if any
    std::string status;
};

class CollectingExporter : public SpanExporter {
public:

    void exportSpan(const SpanData& span) override {
        TraceContext parent{span.context.traceId(), span.parentSpanId, 0};
        int64_t fields = 0;
        for (const auto& [key, value] : span.attributes) {
            if (key == "fdb.fields") {
                fields = std::get<int64_t>(value);
            }
        }
        std::ostringstream status;
        status << span.status;
        std::lock_guard lock(mutex_);
        spans_.push_back({span.name, span.context.traceIdHex(), span.context.spanIdHex(),
                          parent.valid() ? parent.spanIdHex() : std::string{}, fields, status.str()});
    }

    std::vector<ExportedSpan> spans() const {
        std::lock_guard lock(mutex_);
        return spans_;
    }

private:

    mutable std::mutex mutex_;
    std::vector<ExportedSpan> spans_;
};

std::string env(const char* name) {
    const char* v = ::getenv(name);
    ASSERT_MSG(v, std::string("Missing environment variable ") + name);
    return v;
}

std::string spanFile(const std::string& role) {
    return env("FDB_TRACING_TEST_DIR") + "/tracing_" + env("FDB_TRACING_TEST_VARIANT") + "_" + role + ".jsonl";
}

/// Spans exported by a server, skipping the first @p skip lines (written before this run)
std::vector<ExportedSpan> serverSpans(const std::string& role, size_t skip = 0) {
    std::vector<ExportedSpan> out;
    std::ifstream in(spanFile(role));
    std::string line;
    size_t n = 0;
    while (std::getline(in, line)) {
        if (n++ < skip || line.empty()) {
            continue;
        }
        eckit::Value v = eckit::JSONParser::decodeString(line);
        eckit::Value attributes = v["attributes"];
        int64_t fields = attributes.contains("fdb.fields") ? static_cast<long long>(attributes["fdb.fields"]) : 0;
        out.push_back({v["name"], v["trace_id"], v["span_id"], v["parent_span_id"], fields, v["status"]});
    }
    return out;
}

size_t lineCount(const std::string& role) {
    std::ifstream in(spanFile(role));
    size_t n = 0;
    std::string line;
    while (std::getline(in, line)) {
        ++n;
    }
    return n;
}

std::vector<ExportedSpan> inTrace(const std::vector<ExportedSpan>& spans, const std::string& traceId) {
    std::vector<ExportedSpan> out;
    for (const auto& s : spans) {
        if (s.traceId == traceId) {
            out.push_back(s);
        }
    }
    return out;
}

size_t countNamed(const std::vector<ExportedSpan>& spans, const std::string& name) {
    size_t n = 0;
    for (const auto& s : spans) {
        n += (s.name == name) ? 1 : 0;
    }
    return n;
}

/// Total of the fdb.fields attributes of the spans with this name
int64_t fieldsOf(const std::vector<ExportedSpan>& spans, const std::string& name) {
    int64_t n = 0;
    for (const auto& s : spans) {
        n += (s.name == name) ? s.fields : 0;
    }
    return n;
}

Key makeKey(const std::string& date, const std::string& step) {
    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("domain", "g");
    k.set("date", date);
    k.set("type", "fc");
    k.set("levtype", "sfc");
    k.set("param", "167");
    k.set("step", step);
    return k;
}

const std::vector<std::string> dates{"20101010", "20111213"};
const std::vector<std::string> steps{"1", "2"};
const std::string payload = "Tracing payload, tracing payload, tracing payload";

/// Archive, flush, list and retrieve, all as part of the caller's trace
void exercise(FDB& fdb, const TraceContext& caller) {
    for (const auto& d : dates) {
        for (const auto& s : steps) {
            fdb.archive(makeKey(d, s), payload.data(), payload.size(), caller);
        }
    }
    fdb.flush(caller);

    auto request = FDBToolRequest::requestsFromString("class=od,expver=xxxx,stream=oper,time=0000,domain=g")[0];
    ListIterator it = fdb.list(request, ListMode::Deduplicate, 3, caller);
    ListElement elem;
    size_t listed = 0;
    while (it.next(elem)) {
        ++listed;
    }
    EXPECT_EQUAL(listed, dates.size() * steps.size());

    metkit::mars::MarsRequest retrieve = makeKey(dates[0], steps[0]).request("retrieve");
    retrieve.values("date", dates);
    retrieve.values("step", steps);
    std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(retrieve, caller));
    eckit::MemoryHandle out;
    dh->saveInto(out);
    EXPECT_EQUAL(static_cast<size_t>(out.size()), dates.size() * steps.size() * payload.size());
}

/// Poll until @p condition holds (server spans are exported asynchronously), or give up after a while
template <typename F>
bool eventually(F condition, std::chrono::seconds timeout = std::chrono::seconds(20)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return condition();
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Remote trace-context propagation") {

    const std::string variant = env("FDB_TRACING_TEST_VARIANT");
    eckit::Log::info() << "[CLIENT] tracing variant: " << variant << std::endl;

    auto exporter = std::make_shared<CollectingExporter>();
    Tracer::instance().exporter(exporter);
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOn, true});

    const size_t catalogueSkip = lineCount("catalogue");
    const size_t storeSkip = lineCount("store");

    // A fresh trace for each run, so that spans from previous runs never match
    const TraceContext caller{Tracer::newTraceId(), Tracer::newSpanId(), TraceContext::sampledFlag};
    const std::string traceId = caller.traceIdHex();

    // A tracing-disabled FDB, making requests in a caller's trace (e.g. set by fdb_trace_begin()), propagates no
    // context. It is kept alive, so that the FDB below shares the connections it opened: their tracing capability
    // is that of the process, not that of the FDB that happened to open them.
    const TraceContext untraced{Tracer::newTraceId(), Tracer::newSpanId(), TraceContext::sampledFlag};
    std::optional<FDB> disabled;
    if (variant == "enabled") {
        Config config = Config().expandConfig();
        config.set("tracing", false);
        disabled.emplace(config);
        ScopedContext scope(untraced);
        disabled->archive(makeKey(dates[0], steps[0]), payload.data(), payload.size());
        disabled->flush();
    }

    {
        FDB fdb;
        exercise(fdb, caller);
    }
    disabled.reset();

    auto client = inTrace(exporter->spans(), traceId);

    const auto archived = static_cast<int64_t>(dates.size() * steps.size());

    if (variant == "enabled") {

        // one aggregated span for the archives of the flush cycle
        EXPECT_EQUAL(countNamed(client, "fdb.archive"), 1);
        EXPECT_EQUAL(fieldsOf(client, "fdb.archive"), archived);
        EXPECT_EQUAL(countNamed(client, "fdb.flush"), 1);
        EXPECT_EQUAL(countNamed(client, "fdb.list"), 1);
        EXPECT_EQUAL(countNamed(client, "fdb.retrieve"), 1);

        std::set<std::string> clientSpanIds;
        for (const auto& s : client) {
            clientSpanIds.insert(s.spanId);
        }

        // Every server request span is a child of the client span that issued the request
        auto checkServer = [&](const std::string& role, size_t skip, const std::vector<std::string>& expected) {
            EXPECT(eventually([&] {
                auto spans = inTrace(serverSpans(role, skip), traceId);
                for (const auto& name : expected) {
                    if (countNamed(spans, name) == 0) {
                        return false;
                    }
                }
                return true;
            }));

            auto spans = inTrace(serverSpans(role, skip), traceId);
            eckit::Log::info() << "[CLIENT] " << role << ": " << spans.size() << " spans in trace " << traceId
                               << std::endl;
            for (const auto& name : expected) {
                EXPECT(countNamed(spans, name) > 0);
            }
            for (const auto& s : spans) {
                if (s.name.rfind("fdb.server.", 0) == 0) {
                    if (clientSpanIds.count(s.parentSpanId) == 0) {
                        eckit::Log::error() << "[CLIENT] " << role << " span " << s.name << " has parent "
                                            << s.parentSpanId << ", not a client span" << std::endl;
                    }
                    EXPECT(clientSpanIds.count(s.parentSpanId) == 1);
                }
            }
        };

        checkServer("store", storeSkip, {"fdb.server.archive", "fdb.server.flush", "fdb.server.read"});
        checkServer("catalogue", catalogueSkip,
                    {"fdb.server.archive", "fdb.server.flush", "fdb.server.list", "fdb.server.inspect"});

        // The requests of the tracing-disabled FDB (made earlier) reached the servers without its caller's context
        EXPECT(inTrace(exporter->spans(), untraced.traceIdHex()).empty());
        EXPECT(inTrace(serverSpans("store", storeSkip), untraced.traceIdHex()).empty());
        EXPECT(inTrace(serverSpans("catalogue", catalogueSkip), untraced.traceIdHex()).empty());

        // and on each server, one aggregated span per client (i.e. per database: one per date), ended by its flush
        for (const auto& [role, skip] : {std::pair{"store", storeSkip}, std::pair{"catalogue", catalogueSkip}}) {
            auto spans = inTrace(serverSpans(role, skip), traceId);
            EXPECT_EQUAL(countNamed(spans, "fdb.server.archive"), dates.size());
            EXPECT_EQUAL(fieldsOf(spans, "fdb.server.archive"), archived);
        }

        // A read failing on the store server (here, its data file has gone) is recorded as an error
        const TraceContext failing{Tracer::newTraceId(), Tracer::newSpanId(), TraceContext::sampledFlag};
        {
            FDB fdb;
            auto request = FDBToolRequest::requestsFromString(
                "class=od,expver=xxxx,stream=oper,time=0000,domain=g,date=" + dates[1])[0];
            std::set<std::string> files;
            ListIterator it = fdb.list(request, ListMode::Deduplicate, 3);
            ListElement elem;
            while (it.next(elem)) {
                files.insert(elem.uri().path());
            }
            EXPECT(!files.empty());
            for (const auto& f : files) {
                eckit::PathName(f).unlink();
            }

            std::unique_ptr<eckit::DataHandle> dh(
                fdb.retrieve(makeKey(dates[1], steps[0]).request("retrieve"), failing));
            eckit::MemoryHandle out;
            EXPECT_THROWS(dh->saveInto(out));
        }
        EXPECT(eventually([&] {
            for (const auto& span : inTrace(serverSpans("store", storeSkip), failing.traceIdHex())) {
                if (span.name == "fdb.server.read" && span.status == "error") {
                    return true;
                }
            }
            return false;
        }));

        // A flush failing on the store server, in a request which is not sampled: the spans of the request are
        // exported all the same, on the client and on the server. The store server of this test fails the flush of
        // the requests whose trace state asks for it (see fdb_test_remote_failing_store.cc).
        const TraceContext unsampled{Tracer::newTraceId(), Tracer::newSpanId(), 0, "fdbtest=failflush"};
        {
            FDB fdb;
            fdb.archive(makeKey(dates[0], steps[0]), payload.data(), payload.size(), unsampled);
            EXPECT_THROWS(fdb.flush(unsampled));
        }  // n.b. its destructor flushes again, successfully: without a trace context

        auto failed = inTrace(exporter->spans(), unsampled.traceIdHex());
        EXPECT_EQUAL(countNamed(failed, "fdb.archive"), 1);
        EXPECT_EQUAL(countNamed(failed, "fdb.flush"), 1);
        EXPECT(eventually([&] {
            auto spans = inTrace(serverSpans("store", storeSkip), unsampled.traceIdHex());
            return countNamed(spans, "fdb.server.flush") == 1 && countNamed(spans, "fdb.server.archive") == 1;
        }));
        for (const auto& span : inTrace(serverSpans("store", storeSkip), unsampled.traceIdHex())) {
            if (span.name == "fdb.server.flush") {
                EXPECT_EQUAL(span.status, "error");
            }
            if (span.name == "fdb.server.archive") {
                EXPECT_EQUAL(span.status, "unset");  // the archive succeeded: only the flush failed
            }
        }
    }
    else if (variant == "client_disabled") {

        EXPECT(exporter->spans().empty());

        // The servers still trace, but the client's context never reached them
        EXPECT(eventually([&] { return !serverSpans("store", storeSkip).empty(); }));
        EXPECT(inTrace(serverSpans("store", storeSkip), traceId).empty());
        EXPECT(inTrace(serverSpans("catalogue", catalogueSkip), traceId).empty());
    }
    else if (variant == "server_disabled") {

        EXPECT_EQUAL(countNamed(client, "fdb.archive"), 1);
        EXPECT_EQUAL(fieldsOf(client, "fdb.archive"), archived);

        std::this_thread::sleep_for(std::chrono::seconds(1));
        EXPECT(serverSpans("store", storeSkip).empty());
        EXPECT(serverSpans("catalogue", catalogueSkip).empty());
    }
    else {
        throw eckit::BadValue("Unknown FDB_TRACING_TEST_VARIANT " + variant, Here());
    }

    Tracer::instance().exporter(nullptr);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
