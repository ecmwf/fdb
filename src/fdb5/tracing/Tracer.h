// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @date   Nov 2026

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "fdb5/tracing/TraceContext.h"

namespace eckit {
class Configuration;
}

namespace fdb5::tracing {

//----------------------------------------------------------------------------------------------------------------------
// OpenTelemetry-compatible span model.
//
// FDB does not depend on the OpenTelemetry SDK: it creates spans following the OTel data model and W3C context
// propagation, and hands finished spans to a pluggable SpanExporter. An application already using the
// OpenTelemetry SDK can install an exporter that forwards these spans to its TracerProvider.
//----------------------------------------------------------------------------------------------------------------------

enum class SpanKind : uint8_t {
    Internal,
    Server,
    Client
};

enum class StatusCode : uint8_t {
    Unset,
    Ok,
    Error
};

std::ostream& operator<<(std::ostream& s, SpanKind kind);
std::ostream& operator<<(std::ostream& s, StatusCode code);

using AttributeValue = std::variant<bool, int64_t, double, std::string>;

/// A finished (or in-flight) span, as handed to a SpanExporter
struct SpanData {
    std::string name;
    SpanKind kind{SpanKind::Internal};
    TraceContext context;                 // this span's context (trace-id, span-id, flags, tracestate)
    TraceContext::SpanId parentSpanId{};  // all zero for a root span
    std::chrono::system_clock::time_point start;
    std::chrono::system_clock::time_point end;
    std::vector<std::pair<std::string, AttributeValue>> attributes;
    StatusCode status{StatusCode::Unset};
    std::string statusMessage;
};

//----------------------------------------------------------------------------------------------------------------------

class SpanExporter {
public:

    virtual ~SpanExporter() = default;

    /// Called once per finished, sampled span. Must be thread-safe.
    virtual void exportSpan(const SpanData& span) = 0;
};

/// Writes one JSON object per span (one line each), using OTLP field names, to the log: the spans with an error status
/// to eckit::Log::error(), the others to eckit::Log::info(). The log infrastructure forwards them (e.g. to an
/// OpenTelemetry collector).
class LogSpanExporter : public SpanExporter {
public:

    void exportSpan(const SpanData& span) override;

    /// For command line tools, whose standard log channels carry their results: write all the spans to stderr instead,
    /// so that they cannot corrupt (e.g. the JSON output of) the tool. Process-wide.
    static void toStderr(bool enable);

    /// Serialise a span as a single-line JSON object (exposed for testing)
    static void toJSON(std::ostream& out, const SpanData& span);
};

//----------------------------------------------------------------------------------------------------------------------

/// Sampling decision for new spans, following the OpenTelemetry SDK built-in samplers
/// (always_on, always_off, traceidratio and their parentbased_* variants).
class Sampler {
public:

    enum class Kind : uint8_t {
        AlwaysOn,
        AlwaysOff,
        TraceIdRatio
    };

    Sampler() = default;
    Sampler(Kind kind, bool parentBased, double ratio = 1.0);

    /// Parse an OTEL_TRACES_SAMPLER name (e.g. "parentbased_traceidratio") and its argument
    /// @throws eckit::BadValue for unknown names
    static Sampler fromName(const std::string& name, const std::string& arg = {});

    bool shouldSample(const TraceContext& parent, const TraceContext::TraceId& traceId) const;

    std::string name() const;

private:

    Kind kind_{Kind::AlwaysOff};
    bool parentBased_{true};
    double ratio_{1.0};
};

//----------------------------------------------------------------------------------------------------------------------

/// Process-wide tracing configuration.
///
/// Initialised from the environment, then optionally overridden by configure() (called with the 'tracing' block of
/// the first FDB configuration that provides one, by the fdb-server, or by the application). All the settings apply to
/// the whole process, including 'enabled' (also given as a plain 'tracing: false'):
///
///   tracing:
///     enabled: true              # FDB_TRACING_ENABLED (env 'false' always wins: kill switch)
///     sampler: parentbased_always_off   # FDB_TRACING_SAMPLER, then OTEL_TRACES_SAMPLER
///     ratio: 0.01                # FDB_TRACING_SAMPLER_ARG, then OTEL_TRACES_SAMPLER_ARG
///     exporter: log              # none | log (see LogSpanExporter)  FDB_TRACING_EXPORTER
///
/// By default, only the spans of failing requests, and of the requests whose caller samples them, are exported,
/// to the log.
///
/// The failing requests are always traced: the spans which the sampler does not sample are recorded too, and held per
/// trace until the trace has no open span left in this process (i.e. until the request it belongs to is complete). If
/// one of them has an error status, all the held spans of the trace are exported, as is every later span of that
/// request; otherwise they are discarded. Sampled spans are exported as soon as they end.
class Tracer {
public:

    static Tracer& instance();

    /// Apply a 'tracing' configuration block (see class documentation)
    void configure(const eckit::Configuration& config);

    /// Apply the 'tracing' block (or plain 'tracing: false') of an FDB or fdb-server configuration, unless an earlier
    /// configuration already provided one: the first one wins.
    void configureFrom(const eckit::Configuration& fdbConfig);

    /// True unless tracing is disabled, i.e. no span is created and no trace context is sent to remote servers
    bool enabled() const;

    /// True if finished spans are exported (an exporter is installed). When false, spans still propagate
    /// context but record nothing.
    bool exporting() const;

    /// Process-wide override, whatever the configuration (which may install an exporter later): no span is exported,
    /// or even recorded. Trace contexts still propagate. For machine-readable output (e.g. fdb-list --porcelain).
    void suppressExport(bool suppress);

    void exporter(std::shared_ptr<SpanExporter> exporter);
    std::shared_ptr<SpanExporter> exporter() const;

    void sampler(const Sampler& sampler);
    Sampler sampler() const;

    /// At most this many spans are held per unsampled trace ; further successful spans are dropped
    static constexpr size_t maxHeldSpansPerTrace = 10000;

    /// Context for a new span: a child of @p parent if valid, otherwise a new root. Applies the sampler.
    TraceContext newSpanContext(const TraceContext& parent) const;

    void exportSpan(const SpanData& span) const;

    /// Whether a new span with this context is to be recorded. An unsampled span is
    /// held until its trace is complete: every span for which this returns true must then be passed to endSpan().
    bool startSpan(const TraceContext& context);

    /// A request of this trace has failed elsewhere than in a span of this process (or in a span of another trace): its
    /// held spans are exported, as is every later span of the trace, as if one of them had failed. No effect on a trace
    /// which holds nothing (sampled, or without recorded spans).
    void failTrace(const TraceContext::TraceId& traceId);

    /// A recorded span has ended: export it, or hold it until its trace is complete 
    void endSpan(SpanData&& span);

    static TraceContext::TraceId newTraceId();
    static TraceContext::SpanId newSpanId();

private:

    Tracer();

private:

    /// The unsampled spans of a trace recorded in case it fails, until the trace is complete in this process
    struct HeldTrace {
        size_t open{0};      // spans started and not yet ended
        bool failed{false};  // a span has ended with an error status: its spans are exported
        size_t dropped{0};   // spans not held, beyond maxHeldSpansPerTrace
        std::vector<SpanData> spans;
    };

    mutable std::mutex mutex_;
    bool enabled_{true};
    bool envDisabled_{false};
    bool exportSuppressed_{false};

    std::mutex configureMutex_;  // serialises configureFrom(), see there; guards configuredFromConfig_
    bool configuredFromConfig_{false};

    Sampler sampler_;
    std::shared_ptr<SpanExporter> exporter_;

    std::mutex heldMutex_;
    std::map<TraceContext::TraceId, HeldTrace> held_;
};

//----------------------------------------------------------------------------------------------------------------------

/// A span. Creating one generates its context immediately (so it can be propagated); timing and attributes are
/// only recorded if an exporter is installed and the span is sampled (or not: see Tracer). The span ends
/// (and is exported) when end() is called or on destruction. If destroyed during stack unwinding with an unset status,
/// the status is Error.
///
/// A default-constructed Span is a no-op with an invalid context.
class Span {
public:

    Span() = default;

    /// Child of @p parent if valid, otherwise a new root. No-op if tracing is disabled globally.
    Span(std::string name, SpanKind kind, const TraceContext& parent);

    /// Child of the calling thread's current context
    explicit Span(std::string name, SpanKind kind = SpanKind::Internal);

    ~Span();

    Span(Span&& other) noexcept;
    Span& operator=(Span&& other) noexcept;
    Span(const Span&) = delete;
    Span& operator=(const Span&) = delete;

    const TraceContext& context() const { return context_; }

    bool recording() const { return static_cast<bool>(data_); }

    template <typename T>
    void setAttribute(std::string key, T&& value) {
        if (data_) {
            data_->attributes.emplace_back(std::move(key), toAttribute(std::forward<T>(value)));
        }
    }

    void setStatus(StatusCode code, std::string message = {});

    /// By default a span destroyed during stack unwinding, with an unset status, gets an Error status. That only makes
    /// sense for a span destroyed in the scope that created it. Disable it for a span whose ownership moves elsewhere
    /// or which is kept in an object, where the destructor may run for unrelated reasons (e.g. an unrelated exception
    /// unwinding through the object that holds it); the status must then be set explicitly.
    void detectUnwinding(bool enable) { detectUnwinding_ = enable; }

    void end();

    /// End the span with this end time (e.g. that of the last of the operations it covers)
    void end(std::chrono::system_clock::time_point endTime);

private:

    template <typename T>
    static AttributeValue toAttribute(T&& value) {
        using U = std::decay_t<T>;
        if constexpr (std::is_same_v<U, AttributeValue>) {
            return std::forward<T>(value);
        }
        else if constexpr (std::is_same_v<U, bool>) {
            return value;
        }
        else if constexpr (std::is_integral_v<U> || std::is_enum_v<U>) {
            return static_cast<int64_t>(value);
        }
        else if constexpr (std::is_floating_point_v<U>) {
            return static_cast<double>(value);
        }
        else {
            return std::string(std::forward<T>(value));
        }
    }

private:

    TraceContext context_;
    std::unique_ptr<SpanData> data_;
    int uncaughtExceptions_{0};
    bool detectUnwinding_{true};
};

/// A Span that is also the current context of the calling thread for its lifetime. If the span is a no-op
/// (tracing disabled), the current context is left unchanged. It is also the span recordError() applies to.
class ScopedSpan {
public:

    ScopedSpan(std::string name, SpanKind kind, const TraceContext& parent);
    explicit ScopedSpan(std::string name, SpanKind kind = SpanKind::Internal);
    ~ScopedSpan();

    ScopedSpan(const ScopedSpan&) = delete;
    ScopedSpan& operator=(const ScopedSpan&) = delete;

    Span& span() { return span_; }
    const TraceContext& context() const { return span_.context(); }

    template <typename T>
    void setAttribute(std::string key, T&& value) {
        span_.setAttribute(std::move(key), std::forward<T>(value));
    }

private:

    Span span_;
    ScopedContext scope_;
    ScopedSpan* outer_;
};

/// Sets the error status of the innermost ScopedSpan of the calling thread, if any. For code which reports a failure
/// (e.g. in an error reply to a client) instead of throwing it, so that the failure is not lost (see Tracer).
void recordError(std::string_view message);

//----------------------------------------------------------------------------------------------------------------------

/// Aggregates many short operations of the same kind (e.g. the archive calls of a flush cycle) into one span per group
/// and parent context, instead of one span per operation.
///
/// The span of an aggregate starts with its first operation and is the current context of the thread during each of
/// its operations (so that the context propagates, e.g. to a remote server). It ends when end()/endAll() is called
/// (e.g. at the flush), with the end time of its last operation, and carries the totals of its operations:
/// fdb.calls, fdb.fields, fdb.bytes and fdb.busy_ns (the time spent in the operations, as opposed to between them).
///
/// If an operation fails, its aggregate ends as soon as the operation is over, with an error status, and the next
/// operation starts a new aggregate. Thread-safe.
class SpanAggregator {

    struct Aggregate;

public:

    using Attributes = std::vector<std::pair<std::string, AttributeValue>>;

    /// One operation, from construction to destruction. A failure is detected if the operation is left by an
    /// exception, or reported with failed().
    class Operation {
    public:

        /// A no-op if @p aggregator is null
        Operation(SpanAggregator* aggregator, const TraceContext& parent, uint64_t group = 0);
        ~Operation();

        Operation(const Operation&) = delete;
        Operation& operator=(const Operation&) = delete;

        /// Whether the aggregate span records attributes (worth computing those of a failure)
        bool recording() const;

        /// The operation is about to archive @p fields fields: it may write data from now on, which a flush covers
        /// (see SpanAggregator::detachIdle()), before the fields can be recorded by add().
        void begin(size_t fields = 1);

        /// Record the fields archived (and their bytes) by this operation, as they are (announced by begin() or not)
        void add(size_t fields, size_t bytes);

        /// The operation has failed: its aggregate ends with an error status, and these attributes. If called
        /// more than once, the first message is kept and the attributes are added.
        void failed(std::string message, Attributes attributes = {});

    private:

        SpanAggregator* aggregator_;
        std::shared_ptr<Aggregate> aggregate_;
        std::optional<ScopedContext> scope_;
        std::chrono::steady_clock::time_point start_;
        size_t fields_{0};
        size_t bytes_{0};
        size_t begun_{0};
        bool failed_{false};
        std::string failure_;
        Attributes failureAttributes_;
        int uncaughtExceptions_{0};
    };

    SpanAggregator(std::string name, SpanKind kind);
    ~SpanAggregator();

    SpanAggregator(const SpanAggregator&) = delete;
    SpanAggregator& operator=(const SpanAggregator&) = delete;

    /// Aggregates taken out of an aggregator, to end once the operation that closes them is over (e.g. a flush): the
    /// operations which start meanwhile join new aggregates. They end when this is destroyed (those with an operation
    /// in progress, when it is over). If that operation fails, retainTraces() makes sure their traces are exported:
    /// the archive spans (which may belong to other traces than the flush) did not fail, but they are part of a failing
    /// request.
    class Detached {
    public:

        Detached();
        ~Detached();
        Detached(Detached&&) noexcept;
        Detached& operator=(Detached&&) noexcept;
        Detached(const Detached&) = delete;
        Detached& operator=(const Detached&) = delete;

        /// Export the traces of the aggregates, as if one of their spans had failed (see Tracer::failTrace()). Their
        /// status is not changed. Includes those of the aggregates which detachIdle() left open because of an operation
        /// in progress, if fields had been begun since the previous detachIdle(): data that the failing operation was
        /// to make durable.
        void retainTraces();

        /// The flush has succeeded: the fields which the aggregates left open by detachIdle() had finished
        /// archiving by then are covered, and are not the concern of the next flush. Those which were still being
        /// written may or may not be: they remain so.
        void flushed();

    private:

        friend class SpanAggregator;
        std::vector<std::shared_ptr<Aggregate>> aggregates_;

        /// An aggregate left open by detachIdle(), as it was then
        struct LeftOpen {
            std::shared_ptr<Aggregate> aggregate;
            size_t done;    // fields finished (or abandoned), see Aggregate
            bool pending;   // fields were begun since the previous flush
        };
        std::vector<LeftOpen> leftOpen_;
    };

    /// Take the aggregates of a group, including those with an operation in progress (which ends after them): for a
    /// peer which has been acknowledged before the operation is over (e.g. an archive on a server, before its flush).
    Detached detach(uint64_t group);

    /// Take all the aggregates which have no operation in progress: those of an operation which is still running
    /// (e.g. an archive of another thread, which a flush cannot tell to be part of it) stay open, for the next one.
    /// @param flushing  the flush which takes them really flushes: the fields begun until now are covered by it. A
    ///                  flush with nothing to flush covers nothing, and must not tell the fields being written from
    ///                  the others.
    Detached detachIdle(bool flushing = true);

    /// A position in the order of the begun fields, for retainTracesOfNewFields()
    uint64_t mark() const { return beginSequence_; }

    /// Export the traces of the open aggregates in which fields have been begun since the last detachIdle(), and
    /// before @p until (see mark()): for a flush which fails, for the archives which started after detachIdle() but
    /// before the data was flushed, and may have been covered by it. Not those which began after the failure.
    void retainTracesOfNewFields(uint64_t until);

    /// True if an open aggregate has this parent context
    bool usesParent(const TraceContext& parent) const;

    /// End the aggregates of a group, or all of them. Those with an operation in progress end when it is over.
    void end(uint64_t group);
    void endAll();

private:

    using Key = std::tuple<uint64_t, TraceContext::TraceId, TraceContext::SpanId>;

    void detach(const std::shared_ptr<Aggregate>& aggregate);

    /// Take the open aggregates whose key is @p selected (and which have no operation in progress, if @p idleOnly)
    Detached take(const std::function<bool(const Key&)>& selected, bool idleOnly, bool flushing = true);

    const std::string name_;
    const SpanKind kind_;

    mutable std::mutex mutex_;
    std::map<Key, std::shared_ptr<Aggregate>> open_;
    std::atomic<uint64_t> beginSequence_{0};  // counts the fields begun, in all the aggregates
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tracing
