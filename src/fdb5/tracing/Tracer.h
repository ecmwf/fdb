// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @date   Nov 2026

#pragma once

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <string>
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

/// Writes one JSON object per span (one line each), using OTLP field names, to eckit::Log::info() or to a file.
class LogSpanExporter : public SpanExporter {
public:

    /// @param path file to append to; empty means eckit::Log::info()
    explicit LogSpanExporter(const std::string& path = {});
    ~LogSpanExporter() override;

    void exportSpan(const SpanData& span) override;

    /// Serialise a span as a single-line JSON object (exposed for testing)
    static void toJSON(std::ostream& out, const SpanData& span);

private:

    std::mutex mutex_;
    std::unique_ptr<std::ostream> file_;
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
/// the first FDB configuration that provides one, by the fdb-server, or by the application):
///
///   tracing:
///     enabled: true              # FDB_TRACING_ENABLED (env 'false' always wins: kill switch)
///     sampler: parentbased_always_off   # FDB_TRACING_SAMPLER, then OTEL_TRACES_SAMPLER
///     ratio: 0.01                # FDB_TRACING_SAMPLER_ARG, then OTEL_TRACES_SAMPLER_ARG
///     exporter: log              # none | log           FDB_TRACING_EXPORTER
///     path: /path/to/spans.jsonl # log exporter target  FDB_TRACING_LOG_PATH (default: eckit::Log::info())
///     keep_errors: true          # also export the unsampled spans of failing requests  FDB_TRACING_KEEP_ERRORS
///
/// By default, only the spans of failing requests, and of the requests whose caller samples them, are exported,
/// to the info log.
///
/// With keep_errors, the spans which the sampler does not sample are recorded too, and held per trace until the
/// trace has no open span left in this process (i.e. until the request it belongs to is complete). If one of them
/// has an error status, all the held spans of the trace are exported, as is every later span of that request;
/// otherwise they are discarded. Sampled spans are exported as soon as they end, as without keep_errors.
class Tracer {
public:

    static Tracer& instance();

    /// Apply a 'tracing' configuration block (see class documentation)
    void configure(const eckit::Configuration& config);

    /// Apply the process-wide settings (sampler, ratio, exporter, path) of the 'tracing' block of an FDB or
    /// fdb-server configuration, unless an earlier configuration already provided them: the first one wins.
    /// 'enabled' is per-FDB (see enabledIn()) and is not applied globally.
    void configureFrom(const eckit::Configuration& fdbConfig);

    /// True unless tracing has been disabled globally
    bool enabled() const;

    /// True if finished spans are exported (an exporter is installed). When false, spans still propagate
    /// context but record nothing.
    bool exporting() const;

    void exporter(std::shared_ptr<SpanExporter> exporter);
    std::shared_ptr<SpanExporter> exporter() const;

    void sampler(const Sampler& sampler);
    Sampler sampler() const;

    void keepErrors(bool keep);
    bool keepErrors() const;

    /// At most this many spans are held per unsampled trace (see keep_errors); further successful spans are dropped
    static constexpr size_t maxHeldSpansPerTrace = 10000;

    /// Context for a new span: a child of @p parent if valid, otherwise a new root. Applies the sampler.
    TraceContext newSpanContext(const TraceContext& parent) const;

    void exportSpan(const SpanData& span) const;

    /// Whether a new span with this context is to be recorded. An unsampled span recorded because of keep_errors is
    /// held until its trace is complete: every span for which this returns true must then be passed to endSpan().
    bool startSpan(const TraceContext& context);

    /// A recorded span has ended: export it, or hold it until its trace is complete (see keep_errors)
    void endSpan(SpanData&& span);

    static TraceContext::TraceId newTraceId();
    static TraceContext::SpanId newSpanId();

    /// Whether tracing is enabled for an FDB with this configuration: global switch AND 'tracing.enabled' (or a
    /// plain boolean 'tracing: false'), defaulting to true.
    static bool enabledIn(const eckit::Configuration& fdbConfig);

private:

    Tracer();

private:

    /// The unsampled spans of a trace recorded because of keep_errors, until the trace is complete in this process
    struct HeldTrace {
        size_t open{0};      // spans started and not yet ended
        bool failed{false};  // a span has ended with an error status: its spans are exported
        size_t dropped{0};   // spans not held, beyond maxHeldSpansPerTrace
        std::vector<SpanData> spans;
    };

    mutable std::mutex mutex_;
    bool enabled_{true};
    bool envDisabled_{false};
    bool configuredFromConfig_{false};
    bool keepErrors_{true};
    Sampler sampler_;
    std::shared_ptr<SpanExporter> exporter_;

    std::mutex heldMutex_;
    std::map<TraceContext::TraceId, HeldTrace> held_;
};

//----------------------------------------------------------------------------------------------------------------------

/// A span. Creating one generates its context immediately (so it can be propagated); timing and attributes are
/// only recorded if an exporter is installed and the span is sampled (or keep_errors is set, see Tracer). The span ends
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

    void end();

private:

    template <typename T>
    static AttributeValue toAttribute(T&& value) {
        using U = std::decay_t<T>;
        if constexpr (std::is_same_v<U, bool>) {
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
};

/// A Span that is also the current context of the calling thread for its lifetime. If the span is a no-op
/// (tracing disabled), the current context is left unchanged.
class ScopedSpan {
public:

    ScopedSpan(std::string name, SpanKind kind, const TraceContext& parent);
    explicit ScopedSpan(std::string name, SpanKind kind = SpanKind::Internal);

    Span& span() { return span_; }
    const TraceContext& context() const { return span_.context(); }

    template <typename T>
    void setAttribute(std::string key, T&& value) {
        span_.setAttribute(std::move(key), std::forward<T>(value));
    }

private:

    Span span_;
    ScopedContext scope_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tracing
