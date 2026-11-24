// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/tracing/TraceContext.h"

#include <algorithm>
#include <cstring>
#include <ostream>
#include <sstream>
#include <utility>

#include "eckit/exception/Exceptions.h"

namespace fdb5::tracing {

namespace {

/// W3C recommends supporting tracestate values of at least 512 bytes; longer values are dropped
constexpr size_t maxTraceStateLength = 512;

int hexValue(char c) {
    // W3C mandates lowercase hex
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    return -1;
}

template <size_t N>
bool parseHex(std::string_view in, std::array<uint8_t, N>& out) {
    if (in.size() != 2 * N) {
        return false;
    }
    for (size_t i = 0; i < N; ++i) {
        int hi = hexValue(in[2 * i]);
        int lo = hexValue(in[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        out[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

template <size_t N>
std::string toHex(const std::array<uint8_t, N>& in) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string out(2 * N, '0');
    for (size_t i = 0; i < N; ++i) {
        out[2 * i] = digits[in[i] >> 4];
        out[2 * i + 1] = digits[in[i] & 0x0f];
    }
    return out;
}

template <size_t N>
bool allZero(const std::array<uint8_t, N>& a) {
    return std::all_of(a.begin(), a.end(), [](uint8_t b) { return b == 0; });
}

std::string sanitiseTraceState(std::string_view ts) {
    return ts.size() > maxTraceStateLength ? std::string{} : std::string{ts};
}

/// On failure, returns false and leaves the reason in @p reason
bool parseTraceparent(std::string_view tp, std::string_view ts, TraceContext& out, std::string& reason) {

    // version "-" trace-id "-" parent-id "-" trace-flags  ==  2 + 1 + 32 + 1 + 16 + 1 + 2 = 55 chars
    constexpr size_t v0Length = 55;

    if (tp.size() < v0Length) {
        reason = "traceparent too short";
        return false;
    }

    std::array<uint8_t, 1> version{};
    if (!parseHex(tp.substr(0, 2), version) || version[0] == 0xff) {
        reason = "invalid traceparent version";
        return false;
    }
    // Version 00 has exactly 55 characters. Future versions may append fields after a '-' (W3C forward compat).
    if (version[0] == 0x00 ? tp.size() != v0Length : (tp.size() > v0Length && tp[v0Length] != '-')) {
        reason = "invalid traceparent length";
        return false;
    }
    if (tp[2] != '-' || tp[35] != '-' || tp[52] != '-') {
        reason = "invalid traceparent separators";
        return false;
    }

    TraceContext::TraceId traceId;
    TraceContext::SpanId spanId;
    std::array<uint8_t, 1> flags{};

    if (!parseHex(tp.substr(3, 32), traceId) || !parseHex(tp.substr(36, 16), spanId) ||
        !parseHex(tp.substr(53, 2), flags)) {
        reason = "invalid hex digits in traceparent";
        return false;
    }
    if (allZero(traceId) || allZero(spanId)) {
        reason = "all-zero trace-id or parent-id in traceparent";
        return false;
    }

    out = TraceContext{traceId, spanId, flags[0], sanitiseTraceState(ts)};
    return true;
}

thread_local TraceContext currentContext;

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

TraceContext::TraceContext(const TraceId& traceId, const SpanId& spanId, uint8_t flags, std::string traceState) :
    traceId_(traceId), spanId_(spanId), flags_(flags), traceState_(std::move(traceState)) {}

TraceContext TraceContext::fromTraceparent(std::string_view traceparent, std::string_view tracestate) {
    TraceContext ctx;
    std::string reason;
    if (!parseTraceparent(traceparent, tracestate, ctx, reason)) {
        std::ostringstream ss;
        ss << "Invalid W3C traceparent '" << traceparent << "': " << reason;
        throw eckit::BadValue(ss.str(), Here());
    }
    return ctx;
}

TraceContext TraceContext::parse(std::string_view traceparent, std::string_view tracestate) noexcept {
    TraceContext ctx;
    std::string reason;
    try {
        if (parseTraceparent(traceparent, tracestate, ctx, reason)) {
            return ctx;
        }
    }
    catch (...) {
    }
    return {};
}

bool TraceContext::valid() const {
    return !allZero(traceId_) && !allZero(spanId_);
}

std::string TraceContext::traceIdHex() const {
    return toHex(traceId_);
}

std::string TraceContext::spanIdHex() const {
    return toHex(spanId_);
}

std::string TraceContext::traceparent() const {
    if (!valid()) {
        return {};
    }
    std::array<uint8_t, 1> flags{flags_};
    return "00-" + toHex(traceId_) + "-" + toHex(spanId_) + "-" + toHex(flags);
}

TraceContext TraceContext::withSpanId(const SpanId& spanId) const {
    return TraceContext{traceId_, spanId, flags_, traceState_};
}

bool TraceContext::operator==(const TraceContext& other) const {
    return traceId_ == other.traceId_ && spanId_ == other.spanId_ && flags_ == other.flags_ &&
           traceState_ == other.traceState_;
}

std::string TraceContext::encode() const {
    std::string out(binaryFixedSize + traceState_.size(), '\0');
    char* p = out.data();
    *p++ = static_cast<char>(binaryVersion);
    std::memcpy(p, traceId_.data(), traceId_.size());
    p += traceId_.size();
    std::memcpy(p, spanId_.data(), spanId_.size());
    p += spanId_.size();
    *p++ = static_cast<char>(flags_);
    std::memcpy(p, traceState_.data(), traceState_.size());
    return out;
}

TraceContext TraceContext::decode(const void* data, size_t length) {
    if (length < binaryFixedSize) {
        std::ostringstream ss;
        ss << "Trace context too short: " << length << " bytes, expected at least " << binaryFixedSize;
        throw eckit::BadValue(ss.str(), Here());
    }
    const auto* p = static_cast<const uint8_t*>(data);
    if (p[0] != binaryVersion) {
        std::ostringstream ss;
        ss << "Unsupported trace context encoding version " << static_cast<int>(p[0]);
        throw eckit::BadValue(ss.str(), Here());
    }
    ++p;

    TraceId traceId;
    std::memcpy(traceId.data(), p, traceId.size());
    p += traceId.size();
    SpanId spanId;
    std::memcpy(spanId.data(), p, spanId.size());
    p += spanId.size();
    uint8_t flags = *p++;

    std::string_view traceState(reinterpret_cast<const char*>(p), length - binaryFixedSize);

    TraceContext ctx{traceId, spanId, flags, sanitiseTraceState(traceState)};
    if (!ctx.valid()) {
        throw eckit::BadValue("Trace context with all-zero trace-id or span-id", Here());
    }
    return ctx;
}

void TraceContext::print(std::ostream& s) const {
    if (valid()) {
        s << traceparent();
    }
    else {
        s << "<no-trace>";
    }
}

//----------------------------------------------------------------------------------------------------------------------

const TraceContext& current() {
    return currentContext;
}

TraceContext exchangeCurrent(TraceContext ctx) {
    std::swap(currentContext, ctx);
    return ctx;
}

ScopedContext::ScopedContext(TraceContext ctx) : previous_(exchangeCurrent(std::move(ctx))) {}

ScopedContext::~ScopedContext() {
    currentContext = std::move(previous_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tracing
