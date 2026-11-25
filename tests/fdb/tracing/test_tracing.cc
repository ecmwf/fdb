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
#include "eckit/log/Log.h"
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

CASE("Defaults: parent-based always_off sampler, info log exporter") {
    EXPECT_EQUAL(Sampler{}.name(), "parentbased_always_off");
    EXPECT(std::dynamic_pointer_cast<LogSpanExporter>(Tracer::instance().exporter()));
}

CASE("Span: child of an explicit parent keeps trace-id, gets a new span-id") {
    Tracer::instance().exporter(nullptr);
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
        EXPECT(s.recording());  // held until its trace is complete, in case it fails
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // not exported: it did not fail
}

CASE("Failures: the unsampled spans of a successful trace are discarded") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    {
        ScopedSpan root("root");
        EXPECT(!root.context().sampled());
        EXPECT(root.span().recording());
        { ScopedSpan child("child"); }
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);
}

CASE("Failures: all the spans of a failing trace are exported, including those ended before the failure") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});

    TraceContext::TraceId traceId{};
    try {
        ScopedSpan root("root");
        traceId = root.context().traceId();
        { ScopedSpan ok("ok"); }
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // held: the trace is not complete
        ScopedSpan failing("failing");
        throw std::runtime_error("boom");
    }
    catch (const std::runtime_error&) {
    }

    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 3);
    for (const auto& s : spans) {
        EXPECT(s.context.traceId() == traceId);
    }
    EXPECT_EQUAL(spans[0].name, "ok");
    EXPECT(spans[0].status == StatusCode::Unset);
    EXPECT_EQUAL(spans[1].name, "failing");
    EXPECT(spans[1].status == StatusCode::Error);
    EXPECT_EQUAL(spans[2].name, "root");

    // an error status set explicitly counts as well
    {
        Span s("explicit", SpanKind::Internal, TraceContext{});
        s.setStatus(StatusCode::Error, "failed");
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 4);
}

CASE("Failures: sampled spans are exported as they end") {
    ExporterFixture fx;
    {
        ScopedSpan root("root");
        { ScopedSpan child("child"); }
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 2);
}

CASE("Failures: an unsampled caller context is kept if the request fails") {
    ExporterFixture fx;
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    { Span s("ok", SpanKind::Internal, unsampled); }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);
    {
        Span s("failing", SpanKind::Internal, unsampled);
        s.setStatus(StatusCode::Error);
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT(spans[0].parentSpanId == unsampled.spanId());
    EXPECT(!spans[0].context.sampled());
}

CASE("Failures: the spans held per trace are bounded") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    {
        ScopedSpan root("root");
        for (size_t i = 0; i < Tracer::maxHeldSpansPerTrace + 5; ++i) {
            ScopedSpan child("child");
        }
        root.span().setStatus(StatusCode::Error);
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), Tracer::maxHeldSpansPerTrace + 1);
    EXPECT_EQUAL(fx.exporter->spans().back().name, "root");
}

namespace {

int64_t intAttribute(const SpanData& span, const std::string& name) {
    for (const auto& [key, value] : span.attributes) {
        if (key == name) {
            return std::get<int64_t>(value);
        }
    }
    return -1;
}

}  // namespace

CASE("SpanAggregator: one span per group and parent, with the totals, ended by end()") {
    ExporterFixture fx;
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent);
    SpanAggregator aggregator("aggregated", SpanKind::Internal);

    TraceContext aggregateContext;
    for (size_t i = 0; i < 3; ++i) {
        SpanAggregator::Operation op(&aggregator, parent, 1);
        op.add(2, 10);
        aggregateContext = current();  // the aggregate span is current during the operation
    }
    {
        SpanAggregator::Operation op(&aggregator, parent, 2);
        op.add(1, 5);
    }
    EXPECT(aggregateContext.traceId() == parent.traceId());
    EXPECT(aggregateContext.spanId() != parent.spanId());
    EXPECT(!current().valid());
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);

    aggregator.end(1);
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT_EQUAL(spans[0].name, "aggregated");
    EXPECT(spans[0].context.spanId() == aggregateContext.spanId());
    EXPECT(spans[0].parentSpanId == parent.spanId());
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.calls"), 3);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.fields"), 6);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.bytes"), 30);
    EXPECT(intAttribute(spans[0], "fdb.busy_ns") >= 0);

    aggregator.endAll();
    spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 2);
    EXPECT_EQUAL(intAttribute(spans[1], "fdb.calls"), 1);

    // a new aggregate after end()
    { SpanAggregator::Operation op(&aggregator, parent, 1); }
    aggregator.endAll();
    spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 3);
    EXPECT(spans[2].context.spanId() != spans[0].context.spanId());
}

CASE("SpanAggregator: a failing operation ends its aggregate at once, the next one starts a new one") {
    ExporterFixture fx;
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent);

    { SpanAggregator::Operation op(&aggregator, parent); }
    try {
        SpanAggregator::Operation op(&aggregator, parent);
        throw std::runtime_error("boom");
    }
    catch (const std::runtime_error&) {
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT(spans[0].status == StatusCode::Error);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.calls"), 2);

    {
        SpanAggregator::Operation op(&aggregator, parent);
        op.failed("explicit", {{"fdb.key", AttributeValue{std::string{"{param=1}"}}}});
        op.failed("second", {{"fdb.request", AttributeValue{std::string{"retrieve"}}}});
    }
    spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 2);
    EXPECT_EQUAL(spans[1].statusMessage, "explicit");
    EXPECT_EQUAL(spans[1].attributes.size(), 6);  // fdb.key, fdb.request and the four totals

    { SpanAggregator::Operation op(&aggregator, parent); }
    aggregator.endAll();
    spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 3);
    EXPECT(spans[2].status == StatusCode::Unset);
}

CASE("SpanAggregator: concurrent operations, and an aggregate ended while one is in progress") {
    ExporterFixture fx;
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent);

    std::vector<std::thread> threads;
    for (size_t t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            for (size_t i = 0; i < 250; ++i) {
                SpanAggregator::Operation op(&aggregator, parent);
                op.add(1, 1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    {
        SpanAggregator::Operation op(&aggregator, parent);
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // ends when the operation is over
        op.add(1, 1);
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.fields"), 1001);
}

CASE("SpanAggregator: detached aggregates end when released, and export their traces if asked to") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    { SpanAggregator::Operation op(&aggregator, unsampled); }
    {
        auto detached = aggregator.detachIdle();
        // the operations which follow join a new aggregate, not the detached one
        { SpanAggregator::Operation op(&aggregator, unsampled); }
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // held: the trace is not complete
        detached.retainTraces();
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);  // the detached one, in a trace which is exported
    EXPECT(spans[0].status == StatusCode::Unset);  // its status is not changed
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.calls"), 1);

    aggregator.endAll();  // the other one belongs to the failed trace, which is exported as it ends
    EXPECT_EQUAL(fx.exporter->spans().size(), 2);
}

CASE("SpanAggregator: detaching leaves the aggregates of operations in progress open") {
    ExporterFixture fx;
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext parent = TraceContext::fromTraceparent(sampleTraceparent);

    {
        SpanAggregator::Operation running(&aggregator, parent);
        running.add(1, 1);
        { auto detached = aggregator.detachIdle(); }
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // still open
    }
    // idle now: the next flush takes it
    { auto detached = aggregator.detachIdle(); }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.fields"), 1);
}

CASE("SpanAggregator: detaching a group takes the aggregates of the operations in progress too") {
    // e.g. a server which has acknowledged an archive before its operation is over, then fails the flush
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    {
        SpanAggregator::Operation running(&aggregator, unsampled, 7);
        running.add(1, 1);
        {
            auto detached = aggregator.detach(7);
            EXPECT(!aggregator.usesParent(unsampled));
            detached.retainTraces();  // the flush has failed
        }
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // ends with its operation
    }
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);  // exported: its trace was retained
    EXPECT(spans[0].status == StatusCode::Unset);
    EXPECT_EQUAL(intAttribute(spans[0], "fdb.fields"), 1);
}

CASE("SpanAggregator: the traces of the aggregates left open by detachIdle() are retained too") {
    // e.g. a multi-field archive in progress when a flush fails: it has written data which the flush was to make durable
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    {
        SpanAggregator::Operation running(&aggregator, unsampled);
        running.add(1, 1);
        {
            auto detached = aggregator.detachIdle();
            EXPECT(aggregator.usesParent(unsampled));  // left open
            detached.retainTraces();                   // the flush has failed
        }
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);
    }
    { auto detached = aggregator.detachIdle(); }  // the next flush
    EXPECT_EQUAL(fx.exporter->spans().size(), 1);  // exported: its trace was retained
}

CASE("SpanAggregator: the trace of an operation in progress which has archived nothing is not retained") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    {
        SpanAggregator::Operation running(&aggregator, unsampled);
        {
            auto detached = aggregator.detachIdle();
            detached.retainTraces();  // a flush has failed: not because of this archive, which has written nothing
        }
        running.add(1, 1);  // later
    }
    { auto detached = aggregator.detachIdle(); }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);  // it succeeded: nothing exported
}

CASE("SpanAggregator: a field being archived counts, and one covered by a previous flush does not") {
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    // a field which is being written, not yet recorded by add(): a flush may cover it
    {
        ExporterFixture fx;
        SpanAggregator aggregator("aggregated", SpanKind::Internal);
        {
            SpanAggregator::Operation running(&aggregator, unsampled);
            running.begin();
            auto detached = aggregator.detachIdle();
            detached.retainTraces();  // the flush has failed
        }
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);  // exported, in a retained trace
    }

    // a field covered by a flush which succeeded is not the concern of a later flush which fails
    {
        ExporterFixture fx;
        SpanAggregator aggregator("aggregated", SpanKind::Internal);
        {
            SpanAggregator::Operation running(&aggregator, unsampled);
            running.begin();
            running.add(1, 1);
            {
                auto detached = aggregator.detachIdle();
                detached.flushed();  // the flush succeeds
            }
            auto detached = aggregator.detachIdle();
            detached.retainTraces();  // another one fails
        }
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 0);
    }

    // but a field written after the first flush is
    {
        ExporterFixture fx;
        SpanAggregator aggregator("aggregated", SpanKind::Internal);
        {
            SpanAggregator::Operation running(&aggregator, unsampled);
            running.begin();
            running.add(1, 1);
            {
                auto detached = aggregator.detachIdle();
                detached.flushed();  // the flush succeeds
            }
            running.begin();
            auto detached = aggregator.detachIdle();
            detached.retainTraces();  // another one fails
            running.add(1, 1);
        }
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);
    }

    // a field which was still being written when a flush succeeded may have missed it: it remains of concern
    {
        ExporterFixture fx;
        SpanAggregator aggregator("aggregated", SpanKind::Internal);
        {
            SpanAggregator::Operation running(&aggregator, unsampled);
            running.begin();
            {
                auto detached = aggregator.detachIdle();
                detached.flushed();  // the flush succeeds
            }
            {
                auto detached = aggregator.detachIdle();
                detached.retainTraces();  // another one fails, after the field was written
            }
            running.add(1, 1);
        }
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);
    }

    // and one which failed to cover it leaves it to the next
    {
        ExporterFixture fx;
        SpanAggregator aggregator("aggregated", SpanKind::Internal);
        {
            SpanAggregator::Operation running(&aggregator, unsampled);
            running.begin();
            running.add(1, 1);
            { auto detached = aggregator.detachIdle(); }  // the flush fails: not flushed()
            auto detached = aggregator.detachIdle();
            detached.retainTraces();  // the next one fails too
        }
        aggregator.endAll();
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);
    }

    Tracer::instance().sampler(Sampler{});
}

CASE("SpanAggregator: fields begun after detachIdle() and before the failure are retained") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    TraceContext other = TraceContext::fromTraceparent("00-5bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b8-00");
    TraceContext after = TraceContext::fromTraceparent("00-6bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b9-00");

    SpanAggregator::Operation before(&aggregator, other);  // archived nothing yet
    auto detached = aggregator.detachIdle();                // the flush starts

    {
        SpanAggregator::Operation during(&aggregator, unsampled);  // the archive starts during the flush
        during.begin();

        const auto failedAt = aggregator.mark();  // the flush fails
        SpanAggregator::Operation afterwards(&aggregator, after);  // an archive begins after the failure
        afterwards.begin();

        aggregator.retainTracesOfNewFields(failedAt);
    }
    detached = SpanAggregator::Detached{};
    aggregator.endAll();
    auto spans = fx.exporter->spans();
    EXPECT_EQUAL(spans.size(), 1);  // only the archive which began during the flush
    EXPECT(spans[0].context.traceId() == unsampled.traceId());
}

CASE("SpanAggregator: a flush which flushes nothing ends the finished spans and covers nothing") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, true});  // follows the parent
    TraceContext sampled = TraceContext::fromTraceparent("00-5bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b8-01");
    TraceContext unsampled = TraceContext::fromTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");

    SpanAggregator aggregator("aggregated", SpanKind::Internal);
    {
        SpanAggregator::Operation finished(&aggregator, sampled);
        finished.add(1, 1);
    }
    {
        SpanAggregator::Operation running(&aggregator, unsampled);
        running.begin();

        { auto detached = aggregator.detachIdle(false); }  // a flush with nothing to flush
        EXPECT_EQUAL(fx.exporter->spans().size(), 1);      // the finished one ends

        // the field being written is still of concern to the next flush, which fails
        auto detached = aggregator.detachIdle();
        detached.retainTraces();
    }
    aggregator.endAll();
    EXPECT_EQUAL(fx.exporter->spans().size(), 2);  // and the running one is exported, in a retained trace
}

CASE("SpanAggregator: no aggregate is ended early, however many parents: all the traces can be retained") {
    ExporterFixture fx;
    Tracer::instance().sampler(Sampler{Sampler::Kind::AlwaysOff, false});
    SpanAggregator aggregator("aggregated", SpanKind::Internal);

    constexpr size_t n = 2000;
    for (size_t i = 0; i < n; ++i) {
        TraceContext parent{Tracer::newTraceId(), Tracer::newSpanId(), 0};
        SpanAggregator::Operation op(&aggregator, parent);
        op.add(1, 1);
    }
    EXPECT_EQUAL(fx.exporter->spans().size(), 0);

    auto detached = aggregator.detachIdle();
    detached.retainTraces();  // e.g. the flush has failed
    detached = SpanAggregator::Detached{};
    EXPECT_EQUAL(fx.exporter->spans().size(), n);
}

CASE("TraceContext: an oversized tracestate is dropped, however the context is made") {
    const std::string tooLong(TraceContext::maxTraceStateLength + 1, 'a');
    const std::string longest(TraceContext::maxTraceStateLength, 'a');

    TraceContext ctx{Tracer::newTraceId(), Tracer::newSpanId(), 0, tooLong};
    EXPECT(ctx.traceState().empty());
    EXPECT(ctx.encode().size() <= TraceContext::maxBinarySize);

    TraceContext ok{Tracer::newTraceId(), Tracer::newSpanId(), 0, longest};
    EXPECT_EQUAL(ok.traceState(), longest);
    EXPECT_EQUAL(ok.encode().size(), TraceContext::maxBinarySize);

    // also when derived from another context
    EXPECT(ok.withSpanId(Tracer::newSpanId()).traceState() == longest);
}

CASE("SpanAggregator: a null aggregator is a no-op") {
    ExporterFixture fx;
    {
        SpanAggregator::Operation op(nullptr, TraceContext::fromTraceparent(sampleTraceparent));
        op.add(1, 1);
        EXPECT(!op.recording());
        EXPECT(!current().valid());
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

CASE("Log exporter: the spans with an error status to the error log, the others to the info log") {
    SpanData ok;
    ok.name = "ok_span";
    ok.context = TraceContext::fromTraceparent(sampleTraceparent);
    ok.status = StatusCode::Ok;
    SpanData failed = ok;
    failed.name = "failed_span";
    failed.status = StatusCode::Error;

    std::ostringstream info;
    std::ostringstream error;
    // n.b. in a thread of its own, whose log channels are redirected: eckit's log channels are per thread
    std::thread([&] {
        eckit::Log::info().setStream(info);
        eckit::Log::error().setStream(error);
        LogSpanExporter exporter;
        exporter.exportSpan(ok);
        exporter.exportSpan(failed);
    }).join();

    EXPECT(info.str().find("\"name\":\"ok_span\"") != std::string::npos);
    EXPECT(info.str().find("failed_span") == std::string::npos);
    EXPECT(error.str().find("\"name\":\"failed_span\"") != std::string::npos);
    EXPECT(error.str().find("ok_span") == std::string::npos);
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
