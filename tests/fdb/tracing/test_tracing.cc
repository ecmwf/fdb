// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

namespace fdb5::test {

using namespace fdb5::tracing;

namespace {

constexpr const char* sampleTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// Collects exported spans in memory
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

private:

    mutable std::mutex mutex_;
    std::vector<SpanData> spans_;
};

/// Installs a collecting exporter and an always_on sampler for the duration of a test
struct ExporterFixture {
    ExporterFixture() : exporter(std::make_shared<CollectingExporter>()) {
        Tracer::instance().exporter(exporter);
        Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOn, true});
    }
    ~ExporterFixture() {
        Tracer::instance().exporter(nullptr);
        Tracer::instance().sampler(Sampler{});
    }
    std::shared_ptr<CollectingExporter> exporter;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("W3C traceparent: parse and format round trip") {
    TraceContext ctx = TraceContext::fromTraceparent(sampleTraceparent, "vendor=abc");
    EXPECT(ctx.valid());
    EXPECT(ctx.sampled());
    EXPECT_EQUAL(ctx.traceIdHex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    EXPECT_EQUAL(ctx.spanIdHex(), "00f067aa0ba902b7");
    EXPECT_EQUAL(ctx.traceState(), "vendor=abc");
    EXPECT_EQUAL(ctx.traceparent(), std::string{sampleTraceparent});

    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    EXPECT(!unsampled.sampled());
}

CASE("W3C traceparent: invalid inputs are rejected") {
    const std::vector<std::string> invalid{
        "",
        "garbage",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",           // missing flags
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",  // version 00 must be exact
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",        // forbidden version
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",        // zero trace-id
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",        // zero span-id
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",        // uppercase hex
        "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",        // bad separator
    };
    for (const auto& tp : invalid) {
        EXPECT(!TraceContext::parse(tp).valid());
        EXPECT_THROWS_AS(TraceContext::fromTraceparent(tp), eckit::BadValue);
    }

    // Future versions may append fields (W3C forward compatibility)
    EXPECT(TraceContext::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-future").valid());
}

CASE("Default context is invalid") {
    TraceContext ctx;
    EXPECT(!ctx.valid());
    EXPECT(ctx.traceparent().empty());
}

CASE("Binary encoding round trip") {
    TraceContext ctx = TraceContext::fromTraceparent(sampleTraceparent, "k1=v1,k2=v2");
    std::string bin = ctx.encode();
    EXPECT_EQUAL(bin.size(), TraceContext::binaryFixedSize + ctx.traceState().size());

    TraceContext back = TraceContext::decode(bin.data(), bin.size());
    EXPECT(back == ctx);

    TraceContext noState = TraceContext::fromTraceparent(sampleTraceparent);
    std::string bin2 = noState.encode();
    EXPECT_EQUAL(bin2.size(), TraceContext::binaryFixedSize);
    EXPECT(TraceContext::decode(bin2.data(), bin2.size()) == noState);

    // malformed
    EXPECT_THROWS_AS(TraceContext::decode(bin.data(), 10), eckit::BadValue);
    std::string badVersion = bin;
    badVersion[0] = 2;
    EXPECT_THROWS_AS(TraceContext::decode(badVersion.data(), badVersion.size()), eckit::BadValue);
    std::string zeros(TraceContext::binaryFixedSize, '\0');
    zeros[0] = static_cast<char>(TraceContext::binaryVersion);
    EXPECT_THROWS_AS(TraceContext::decode(zeros.data(), zeros.size()), eckit::BadValue);
}

CASE("ScopedContext nests and restores") {
    EXPECT(!current().valid());
    TraceContext a = TraceContext::fromTraceparent(sampleTraceparent);
    {
        ScopedContext sa(a);
        EXPECT(current() == a);
        {
            ScopedContext cleared{TraceContext{}};
            EXPECT(!current().valid());
        }
        EXPECT(current() == a);
    }
    EXPECT(!current().valid());
}

CASE("Current context is thread local") {
    ScopedContext sa(TraceContext::fromTraceparent(sampleTraceparent));
    bool otherThreadValid = true;
    std::thread t([&] { otherThreadValid = current().valid(); });
    t.join();
    EXPECT(!otherThreadValid);
    EXPECT(current().valid());
}

CASE("Span: child of an explicit parent keeps trace-id, gets a new span-id") {
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent, "vendor=abc");
    Span span("test", SpanKind::Internal, parent);
    EXPECT(span.context().valid());
    EXPECT(span.context().traceId() == parent.traceId());
    EXPECT(span.context().spanId() != parent.spanId());
    EXPECT(span.context().sampled());
    EXPECT_EQUAL(span.context().traceState(), "vendor=abc");
    // no exporter installed: context propagates, nothing is recorded
    EXPECT(!span.recording());
}

CASE("Span: no parent creates a new root") {
    Span a("a", SpanKind::Internal, TraceContext{});
    Span b("b", SpanKind::Internal, TraceContext{});
    EXPECT(a.context().valid());
    EXPECT(b.context().valid());
    EXPECT(a.context().traceId() != b.context().traceId());
}

CASE("ScopedSpan becomes the current context and nested spans are its children") {
    ExporterFixture fx;
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent);
    {
        ScopedSpan outer("outer", SpanKind::Internal, parent);
        EXPECT(current() == outer.context());
        outer.setAttribute("fdb.fields", 3);
        outer.setAttribute("fdb.name", "abc");
        {
            ScopedSpan inner("inner");
            EXPECT(inner.context().traceId() == parent.traceId());
            EXPECT(current() == inner.context());
        }
        EXPECT(current() == outer.context());
    }
    EXPECT(!current().valid());

    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 2);
    // inner finishes first
    EXPECT_EQUAL(spans[0].name, "inner");
    EXPECT_EQUAL(spans[1].name, "outer");
    EXPECT(spans[0].parentSpanId == spans[1].context.spanId());
    EXPECT(spans[1].parentSpanId == parent.spanId());
    EXPECT(spans[1].end >= spans[1].start);
    EXPECT_EQUAL(spans[1].attributes.size(), 2);
    EXPECT(std::get<int64_t>(spans[1].attributes[0].second) == 3);
    EXPECT(std::get<std::string>(spans[1].attributes[1].second) == "abc");
}

CASE("Span ended by an exception gets an error status") {
    ExporterFixture fx;
    try {
        ScopedSpan s("failing");
        throw std::runtime_error("boom");
    }
    catch (const std::runtime_error&) {
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT(spans[0].status == StatusCode::Error);
}

CASE("Parent-based sampling follows an unsampled parent") {
    ExporterFixture fx;
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    {
        Span s("unsampled", SpanKind::Internal, unsampled);
        EXPECT(s.context().valid());  // still propagated
        EXPECT(!s.context().sampled());
        EXPECT(!s.recording());
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);
}

CASE("Sampler names and ratio") {
    EXPECT_EQUAL(Sampler::fromName("parentbased_always_on").name(), "parentbased_always_on");
    EXPECT_EQUAL(Sampler::fromName("always_off").name(), "always_off");
    EXPECT_THROWS_AS(Sampler::fromName("nonsense"), eckit::BadValue);
    EXPECT_THROWS_AS(Sampler::fromName("traceidratio", "2"), eckit::BadValue);

    Sampler none = Sampler::fromName("traceidratio", "0");
    Sampler all = Sampler::fromName("traceidratio", "1");
    Sampler half = Sampler::fromName("traceidratio", "0.5");
    size_t sampled = 0;
    const size_t n = 4000;
    for (size_t i = 0; i < n; ++i) {
        auto id = Tracer::newTraceId();
        EXPECT(!none.shouldSample({}, id));
        EXPECT(all.shouldSample({}, id));
        sampled += half.shouldSample({}, id) ? 1 : 0;
    }
    EXPECT(sampled > n / 2 - n / 10 && sampled < n / 2 + n / 10);
}

CASE("Per-FDB enablement from configuration") {
    eckit::LocalConfiguration none;
    EXPECT(Tracer::enabledIn(none));

    eckit::LocalConfiguration off;
    off.set("tracing", false);
    EXPECT(!Tracer::enabledIn(off));

    eckit::LocalConfiguration block;
    eckit::LocalConfiguration tracing;
    tracing.set("enabled", false);
    block.set("tracing", tracing);
    EXPECT(!Tracer::enabledIn(block));
}

CASE("Log exporter writes one JSON object with OTLP field names") {
    SpanData data;
    data.name = "fdb.archive";
    data.kind = SpanKind::Client;
    data.context = TraceContext::fromTraceparent(sampleTraceparent);
    data.parentSpanId = Tracer::newSpanId();
    data.attributes.emplace_back("fdb.bytes", int64_t{42});
    data.status = StatusCode::Ok;

    std::ostringstream out;
    LogSpanExporter::toJSON(out, data);
    std::string json = out.str();
    EXPECT(json.find('\n') == std::string::npos);
    EXPECT(json.find("\"name\":\"fdb.archive\"") != std::string::npos);
    EXPECT(json.find("\"kind\":\"client\"") != std::string::npos);
    EXPECT(json.find("\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\"") != std::string::npos);
    EXPECT(json.find("\"fdb.bytes\":42") != std::string::npos);
    EXPECT(json.find("\"status\":\"ok\"") != std::string::npos);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
