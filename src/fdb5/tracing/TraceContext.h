// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @date   Nov 2026

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <string_view>

namespace fdb5::tracing {

//----------------------------------------------------------------------------------------------------------------------

/// A W3C Trace Context (https://www.w3.org/TR/trace-context/), i.e. the information OpenTelemetry propagates
/// between processes: a 16-byte trace-id, the 8-byte id of the parent span, the trace-flags and an opaque
/// vendor-specific tracestate.
///
/// A default-constructed TraceContext is invalid ("no context").
///
/// Callers instrumented with OpenTelemetry pass their current context by injecting it with the W3C
/// TraceContextPropagator and handing the resulting 'traceparent' (and optionally 'tracestate') strings to
/// TraceContext::fromTraceparent().
class TraceContext {

public:  // types

    using TraceId = std::array<uint8_t, 16>;
    using SpanId = std::array<uint8_t, 8>;

    static constexpr uint8_t sampledFlag = 0x01;

public:  // methods

    TraceContext() = default;

    TraceContext(const TraceId& traceId, const SpanId& spanId, uint8_t flags, std::string traceState = {});

    /// Parse a W3C 'traceparent' header value ("00-<32 hex>-<16 hex>-<2 hex>") and an optional 'tracestate'.
    /// @throws eckit::BadValue if the traceparent is malformed or carries all-zero ids.
    static TraceContext fromTraceparent(std::string_view traceparent, std::string_view tracestate = {});

    /// As fromTraceparent(), but returns an invalid context instead of throwing, as required by the W3C
    /// specification for propagated headers.
    static TraceContext parse(std::string_view traceparent, std::string_view tracestate = {}) noexcept;

    bool valid() const;
    bool sampled() const { return (flags_ & sampledFlag) != 0; }

    const TraceId& traceId() const { return traceId_; }
    const SpanId& spanId() const { return spanId_; }
    uint8_t flags() const { return flags_; }
    const std::string& traceState() const { return traceState_; }

    std::string traceIdHex() const;
    std::string spanIdHex() const;

    /// The W3C 'traceparent' representation (empty string if invalid)
    std::string traceparent() const;

    /// Same trace, same flags and tracestate, different span-id
    TraceContext withSpanId(const SpanId& spanId) const;

    bool operator==(const TraceContext& other) const;
    bool operator!=(const TraceContext& other) const { return !(*this == other); }

    // -------------- Binary encoding (FDB remote protocol, "Tracing" version 1) ------------------------------------

    /// Layout: version(1) | trace-id(16) | span-id(8) | flags(1) | tracestate (remaining bytes, may be empty)
    static constexpr uint8_t binaryVersion = 1;
    static constexpr size_t binaryFixedSize = 1 + 16 + 8 + 1;

    /// W3C recommends supporting tracestate values of at least 512 bytes; longer values are dropped
    static constexpr size_t maxTraceStateLength = 512;

    /// The largest encoded context
    static constexpr size_t maxBinarySize = binaryFixedSize + maxTraceStateLength;

    std::string encode() const;

    /// @throws eckit::BadValue on malformed input
    static TraceContext decode(const void* data, size_t length);

private:  // methods

    void print(std::ostream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const TraceContext& ctx) {
        ctx.print(s);
        return s;
    }

private:  // members

    TraceId traceId_{};
    SpanId spanId_{};
    uint8_t flags_{0};
    std::string traceState_;
};

//----------------------------------------------------------------------------------------------------------------------

/// The trace context active on the calling thread (invalid if none)
const TraceContext& current();

/// RAII: make @p ctx the current context of the calling thread, restoring the previous one on destruction.
/// An invalid @p ctx clears the current context for the lifetime of the scope.
class ScopedContext {
public:

    explicit ScopedContext(TraceContext ctx);
    ~ScopedContext();

    ScopedContext(const ScopedContext&) = delete;
    ScopedContext& operator=(const ScopedContext&) = delete;
    ScopedContext(ScopedContext&&) = delete;
    ScopedContext& operator=(ScopedContext&&) = delete;

private:

    TraceContext previous_;
};

/// Low-level replacement of the current context, returning the previous one. Prefer ScopedContext.
/// Used where the lifetime of the context cannot be expressed as a C++ scope (e.g. the C API).
TraceContext exchangeCurrent(TraceContext ctx);

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tracing
