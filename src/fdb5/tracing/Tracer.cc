// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/tracing/Tracer.h"

#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <ostream>
#include <random>
#include <sstream>
#include <thread>

#include "eckit/config/Configuration.h"
#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Log.h"
#include "eckit/runtime/Main.h"
#include "eckit/utils/StringTools.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb5.h"

namespace fdb5::tracing {

namespace {

std::mt19937_64& rng() {
    thread_local std::mt19937_64 gen{[] {
        std::random_device rd;
        std::seed_seq seq{rd(),
                          rd(),
                          rd(),
                          rd(),
                          static_cast<unsigned>(::getpid()),
                          static_cast<unsigned>(std::hash<std::thread::id>{}(std::this_thread::get_id())),
                          static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count())};
        return std::mt19937_64{seq};
    }()};
    return gen;
}

template <size_t N>
void fillRandomNonZero(std::array<uint8_t, N>& out) {
    do {
        for (size_t i = 0; i < N; i += 8) {
            uint64_t r = rng()();
            std::memcpy(out.data() + i, &r, std::min<size_t>(8, N - i));
        }
    } while (std::all_of(out.begin(), out.end(), [](uint8_t b) { return b == 0; }));
}

const char* env(const char* name) {
    const char* v = ::getenv(name);
    return (v && *v) ? v : nullptr;
}

const char* env(const char* name, const char* fallback) {
    const char* v = env(name);
    return v ? v : env(fallback);
}

bool parseBool(const std::string& s) {
    std::string l = eckit::StringTools::lower(eckit::StringTools::trim(s));
    return !(l == "0" || l == "false" || l == "off" || l == "no");
}

std::shared_ptr<SpanExporter> makeExporter(const std::string& name) {
    std::string n = eckit::StringTools::lower(name);
    if (n.empty() || n == "none") {
        return nullptr;
    }
    if (n == "log") {
        return std::make_shared<LogSpanExporter>();
    }
    std::ostringstream ss;
    ss << "Unknown tracing exporter '" << name << "' (supported: none, log)";
    throw eckit::BadValue(ss.str(), Here());
}

int64_t unixNanos(std::chrono::system_clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

const std::string& serviceName() {
    static const std::string name = [] {
        if (const char* n = env("OTEL_SERVICE_NAME")) {
            return std::string{n};
        }
        try {
            return eckit::Main::instance().name();
        }
        catch (...) {
            return std::string{"fdb"};
        }
    }();
    return name;
}

const std::string& hostName() {
    static const std::string name = [] {
        char buf[256] = {0};
        ::gethostname(buf, sizeof(buf) - 1);
        return std::string{buf};
    }();
    return name;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& s, SpanKind kind) {
    switch (kind) {
        case SpanKind::Internal:
            return s << "internal";
        case SpanKind::Server:
            return s << "server";
        case SpanKind::Client:
            return s << "client";
    }
    return s << "unknown";
}

std::ostream& operator<<(std::ostream& s, StatusCode code) {
    switch (code) {
        case StatusCode::Unset:
            return s << "unset";
        case StatusCode::Ok:
            return s << "ok";
        case StatusCode::Error:
            return s << "error";
    }
    return s << "unknown";
}

//----------------------------------------------------------------------------------------------------------------------

void LogSpanExporter::toJSON(std::ostream& out, const SpanData& span) {
    std::ostringstream kind;
    kind << span.kind;
    std::ostringstream status;
    status << span.status;
    TraceContext parent{span.context.traceId(), span.parentSpanId, 0};

    eckit::JSON j(out);
    j.startObject();
    j << "name" << span.name;
    j << "kind" << kind.str();
    j << "trace_id" << span.context.traceIdHex();
    j << "span_id" << span.context.spanIdHex();
    j << "parent_span_id" << (parent.valid() ? parent.spanIdHex() : std::string{});
    j << "trace_flags" << static_cast<int>(span.context.flags());
    if (!span.context.traceState().empty()) {
        j << "trace_state" << span.context.traceState();
    }
    j << "start_time_unix_nano" << static_cast<long long>(unixNanos(span.start));
    j << "end_time_unix_nano" << static_cast<long long>(unixNanos(span.end));
    j << "status" << status.str();
    if (!span.statusMessage.empty()) {
        j << "status_message" << span.statusMessage;
    }
    j << "attributes";
    j.startObject();
    for (const auto& [key, value] : span.attributes) {
        j << key;
        std::visit(
            [&j](const auto& v) {
                using V = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<V, int64_t>) {
                    j << static_cast<long long>(v);
                }
                else {
                    j << v;
                }
            },
            value);
    }
    j.endObject();
    j << "resource";
    j.startObject();
    j << "service.name" << serviceName();
    j << "host.name" << hostName();
    j << "process.pid" << static_cast<long long>(::getpid());
    j.endObject();
    j.endObject();
}

void LogSpanExporter::exportSpan(const SpanData& span) {
    std::ostringstream line;
    toJSON(line, span);

    // n.b. eckit's log channels are per thread
    std::ostream& log = span.status == StatusCode::Error ? eckit::Log::error() : eckit::Log::info();
    log << line.str() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

Sampler::Sampler(Kind kind, bool parentBased, double ratio) : kind_(kind), parentBased_(parentBased), ratio_(ratio) {
    if (!(ratio_ >= 0.0 && ratio_ <= 1.0)) {
        std::ostringstream ss;
        ss << "Tracing sampler ratio must be within [0, 1], got " << ratio;
        throw eckit::BadValue(ss.str(), Here());
    }
}

Sampler Sampler::fromName(const std::string& name, const std::string& arg) {
    std::string n = eckit::StringTools::lower(eckit::StringTools::trim(name));
    bool parentBased = false;
    const std::string prefix = "parentbased_";
    if (n.rfind(prefix, 0) == 0) {
        parentBased = true;
        n = n.substr(prefix.size());
    }

    if (n == "always_on") {
        return {Kind::AlwaysOn, parentBased};
    }
    if (n == "always_off") {
        return {Kind::AlwaysOff, parentBased};
    }
    if (n == "traceidratio") {
        double ratio = arg.empty() ? 1.0 : eckit::Translator<std::string, double>()(arg);
        return {Kind::TraceIdRatio, parentBased, ratio};
    }

    std::ostringstream ss;
    ss << "Unknown tracing sampler '" << name << "'";
    throw eckit::BadValue(ss.str(), Here());
}

bool Sampler::shouldSample(const TraceContext& parent, const TraceContext::TraceId& traceId) const {
    if (parentBased_ && parent.valid()) {
        return parent.sampled();
    }
    switch (kind_) {
        case Kind::AlwaysOn:
            return true;
        case Kind::AlwaysOff:
            return false;
        case Kind::TraceIdRatio: {
            // As in the OpenTelemetry SDK: compare the lower 8 bytes of the trace-id against the ratio, so that
            // every participant of a trace takes the same decision
            uint64_t v = 0;
            for (size_t i = 8; i < 16; ++i) {
                v = (v << 8) | traceId[i];
            }
            v >>= 1;  // 63 bits
            const auto bound = static_cast<uint64_t>(ratio_ * static_cast<double>(1ULL << 63));
            return v < bound;
        }
    }
    return false;
}

std::string Sampler::name() const {
    std::string n = parentBased_ ? "parentbased_" : "";
    switch (kind_) {
        case Kind::AlwaysOn:
            return n + "always_on";
        case Kind::AlwaysOff:
            return n + "always_off";
        case Kind::TraceIdRatio:
            return n + "traceidratio(" + std::to_string(ratio_) + ")";
    }
    return n + "unknown";
}

//----------------------------------------------------------------------------------------------------------------------

Tracer& Tracer::instance() {
    static Tracer tracer;
    return tracer;
}

Tracer::Tracer() : exporter_(std::make_shared<LogSpanExporter>()) {
    if (const char* e = env("FDB_TRACING_ENABLED")) {
        enabled_ = parseBool(e);
        envDisabled_ = !enabled_;
    }

    try {
        if (const char* s = env("FDB_TRACING_SAMPLER", "OTEL_TRACES_SAMPLER")) {
            const char* a = env("FDB_TRACING_SAMPLER_ARG", "OTEL_TRACES_SAMPLER_ARG");
            sampler_ = Sampler::fromName(s, a ? a : "");
        }
        if (const char* k = env("FDB_TRACING_KEEP_ERRORS")) {
            keepErrors_ = parseBool(k);
        }
        if (const char* x = env("FDB_TRACING_EXPORTER")) {
            exporter_ = makeExporter(x);
        }
    }
    catch (const std::exception& e) {
        // Never fail the application because of a tracing misconfiguration in the environment
        eckit::Log::warning() << "FDB tracing: ignoring invalid environment configuration: " << e.what() << std::endl;
    }
}

void Tracer::configureFrom(const eckit::Configuration& fdbConfig) {
    if (!fdbConfig.has("tracing")) {
        return;
    }
    eckit::LocalConfiguration tracing;
    if (fdbConfig.isBoolean("tracing")) {
        tracing.set("enabled", fdbConfig.getBool("tracing"));
    }
    else {
        tracing = fdbConfig.getSubConfiguration("tracing");
        if (!(tracing.has("enabled") || tracing.has("sampler") || tracing.has("exporter") ||
              tracing.has("keep_errors"))) {
            return;
        }
    }
    {
        std::lock_guard lock(mutex_);
        if (configuredFromConfig_) {
            return;
        }
        configuredFromConfig_ = true;
    }
    configure(tracing);
}

void Tracer::configure(const eckit::Configuration& config) {

    std::lock_guard lock(mutex_);

    if (config.has("enabled")) {
        enabled_ = config.getBool("enabled") && !envDisabled_;
    }
    if (config.has("sampler")) {
        std::string arg;
        if (config.has("ratio")) {
            arg = std::to_string(config.getDouble("ratio"));
        }
        sampler_ = Sampler::fromName(config.getString("sampler"), arg);
    }
    if (config.has("exporter")) {
        exporter_ = makeExporter(config.getString("exporter"));
    }
    if (config.has("keep_errors")) {
        keepErrors_ = config.getBool("keep_errors");
    }

    LOG_DEBUG_LIB(LibFdb5) << "FDB tracing configured: enabled=" << enabled_ << ", sampler=" << sampler_.name()
                           << ", keep_errors=" << keepErrors_ << ", exporting=" << static_cast<bool>(exporter_)
                           << std::endl;
}

bool Tracer::enabled() const {
    std::lock_guard lock(mutex_);
    return enabled_;
}

bool Tracer::exporting() const {
    std::lock_guard lock(mutex_);
    return static_cast<bool>(exporter_);
}

void Tracer::exporter(std::shared_ptr<SpanExporter> exporter) {
    std::lock_guard lock(mutex_);
    exporter_ = std::move(exporter);
}

std::shared_ptr<SpanExporter> Tracer::exporter() const {
    std::lock_guard lock(mutex_);
    return exporter_;
}

void Tracer::sampler(const Sampler& sampler) {
    std::lock_guard lock(mutex_);
    sampler_ = sampler;
}

Sampler Tracer::sampler() const {
    std::lock_guard lock(mutex_);
    return sampler_;
}

void Tracer::keepErrors(bool keep) {
    std::lock_guard lock(mutex_);
    keepErrors_ = keep;
}

bool Tracer::keepErrors() const {
    std::lock_guard lock(mutex_);
    return keepErrors_;
}

TraceContext Tracer::newSpanContext(const TraceContext& parent) const {
    const TraceContext::TraceId traceId = parent.valid() ? parent.traceId() : newTraceId();
    const bool sampled = sampler().shouldSample(parent, traceId);

    uint8_t flags = parent.valid() ? parent.flags() : 0;
    flags = sampled ? (flags | TraceContext::sampledFlag) : (flags & ~TraceContext::sampledFlag);

    return TraceContext{traceId, newSpanId(), flags, parent.valid() ? parent.traceState() : std::string{}};
}

void Tracer::exportSpan(const SpanData& span) const {
    if (auto e = exporter()) {
        try {
            e->exportSpan(span);
        }
        catch (const std::exception& ex) {
            eckit::Log::warning() << "FDB tracing: failed to export span " << span.name << ": " << ex.what()
                                  << std::endl;
        }
    }
}

bool Tracer::startSpan(const TraceContext& context) {
    {
        std::lock_guard lock(mutex_);
        if (!exporter_) {
            return false;
        }
        if (context.sampled()) {
            return true;
        }
        if (!keepErrors_) {
            return false;
        }
    }
    std::lock_guard lock(heldMutex_);
    ++held_[context.traceId()].open;
    return true;
}

void Tracer::endSpan(SpanData&& span) {
    if (span.context.sampled()) {
        exportSpan(span);
        return;
    }

    // Unsampled, recorded because of keep_errors
    const bool failed = span.status == StatusCode::Error;
    std::vector<SpanData> toExport;
    size_t dropped = 0;
    {
        std::lock_guard lock(heldMutex_);
        auto it = held_.find(span.context.traceId());
        if (it == held_.end()) {
            // Started before keep_errors was set
            if (failed) {
                toExport.push_back(std::move(span));
            }
        }
        else {
            HeldTrace& trace = it->second;
            if (failed && !trace.failed) {
                trace.failed = true;
                toExport = std::move(trace.spans);
                dropped = trace.dropped;
            }
            if (trace.failed) {
                toExport.push_back(std::move(span));
            }
            else if (trace.spans.size() < maxHeldSpansPerTrace) {
                trace.spans.push_back(std::move(span));
            }
            else {
                ++trace.dropped;
            }
            if (--trace.open == 0) {
                held_.erase(it);
            }
        }
    }

    if (dropped != 0) {
        eckit::Log::warning() << "FDB tracing: " << dropped << " spans of failed trace "
                              << toExport.back().context.traceIdHex() << " were not kept (more than "
                              << maxHeldSpansPerTrace << " spans held)" << std::endl;
    }
    for (const auto& s : toExport) {
        exportSpan(s);
    }
}

TraceContext::TraceId Tracer::newTraceId() {
    TraceContext::TraceId id;
    fillRandomNonZero(id);
    return id;
}

TraceContext::SpanId Tracer::newSpanId() {
    TraceContext::SpanId id;
    fillRandomNonZero(id);
    return id;
}

//----------------------------------------------------------------------------------------------------------------------

Span::Span(std::string name, SpanKind kind, const TraceContext& parent) {
    Tracer& tracer = Tracer::instance();
    if (!tracer.enabled()) {
        return;
    }

    context_ = tracer.newSpanContext(parent);
    uncaughtExceptions_ = std::uncaught_exceptions();

    if (tracer.startSpan(context_)) {
        data_ = std::make_unique<SpanData>();
        data_->name = std::move(name);
        data_->kind = kind;
        data_->context = context_;
        if (parent.valid()) {
            data_->parentSpanId = parent.spanId();
        }
        data_->start = std::chrono::system_clock::now();
    }
}

Span::Span(std::string name, SpanKind kind) : Span(std::move(name), kind, current()) {}

Span::~Span() {
    if (data_ && data_->status == StatusCode::Unset && std::uncaught_exceptions() > uncaughtExceptions_) {
        data_->status = StatusCode::Error;
        data_->statusMessage = "exception";
    }
    end();
}

Span::Span(Span&& other) noexcept :
    context_(std::move(other.context_)), data_(std::move(other.data_)), uncaughtExceptions_(other.uncaughtExceptions_) {
    other.context_ = TraceContext{};
}

Span& Span::operator=(Span&& other) noexcept {
    if (this != &other) {
        end();
        context_ = std::move(other.context_);
        data_ = std::move(other.data_);
        uncaughtExceptions_ = other.uncaughtExceptions_;
        other.context_ = TraceContext{};
    }
    return *this;
}

void Span::setStatus(StatusCode code, std::string message) {
    if (data_) {
        data_->status = code;
        data_->statusMessage = std::move(message);
    }
}

void Span::end() {
    end(std::chrono::system_clock::now());
}

void Span::end(std::chrono::system_clock::time_point endTime) {
    if (data_) {
        data_->end = endTime;
        std::unique_ptr<SpanData> data = std::move(data_);
        Tracer::instance().endSpan(std::move(*data));
    }
}

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// The innermost ScopedSpan of the thread (see recordError())
thread_local ScopedSpan* innermostScopedSpan = nullptr;

}  // namespace

ScopedSpan::ScopedSpan(std::string name, SpanKind kind, const TraceContext& parent) :
    span_(std::move(name), kind, parent),
    scope_(span_.context().valid() ? span_.context() : current()),
    outer_(innermostScopedSpan) {
    innermostScopedSpan = this;
}

ScopedSpan::~ScopedSpan() {
    innermostScopedSpan = outer_;
}

void recordError(std::string_view message) {
    if (innermostScopedSpan) {
        innermostScopedSpan->span().setStatus(StatusCode::Error, std::string{message});
    }
}

ScopedSpan::ScopedSpan(std::string name, SpanKind kind) : ScopedSpan(std::move(name), kind, current()) {}

//----------------------------------------------------------------------------------------------------------------------

struct SpanAggregator::Aggregate {

    Aggregate(Key k, std::string name, SpanKind kind, const TraceContext& parent) :
        key(std::move(k)), span(std::move(name), kind, parent) {}

    ~Aggregate() {
        span.setAttribute("fdb.calls", calls);
        span.setAttribute("fdb.fields", fields);
        span.setAttribute("fdb.bytes", bytes);
        span.setAttribute("fdb.busy_ns", busy.count());
        // n.b. ended explicitly: the destructor of a Span marks it as failed if it runs during stack unwinding
        span.end(calls != 0 ? lastEnd : std::chrono::system_clock::now());
    }

    const Key key;
    Span span;

    std::mutex mutex;
    size_t calls{0};
    size_t fields{0};
    size_t bytes{0};
    std::chrono::nanoseconds busy{0};
    std::chrono::system_clock::time_point lastEnd;
    bool failed{false};
};

SpanAggregator::SpanAggregator(std::string name, SpanKind kind) : name_(std::move(name)), kind_(kind) {}

SpanAggregator::~SpanAggregator() {
    endAll();
}

void SpanAggregator::end(uint64_t group) {
    std::vector<std::shared_ptr<Aggregate>> ended;  // n.b. released (and exported) outside the lock
    std::lock_guard lock(mutex_);
    for (auto it = open_.begin(); it != open_.end();) {
        if (std::get<0>(it->first) == group) {
            ended.push_back(std::move(it->second));
            it = open_.erase(it);
        }
        else {
            ++it;
        }
    }
}

void SpanAggregator::endAll() {
    std::map<Key, std::shared_ptr<Aggregate>> ended;
    std::lock_guard lock(mutex_);
    std::swap(ended, open_);
}

void SpanAggregator::detach(const std::shared_ptr<Aggregate>& aggregate) {
    std::lock_guard lock(mutex_);
    if (auto it = open_.find(aggregate->key); it != open_.end() && it->second == aggregate) {
        open_.erase(it);
    }
}

SpanAggregator::Operation::Operation(SpanAggregator* aggregator, const TraceContext& parent, uint64_t group) :
    aggregator_(aggregator) {
    if (!aggregator_) {
        return;
    }
    start_ = std::chrono::steady_clock::now();
    uncaughtExceptions_ = std::uncaught_exceptions();

    Key key{group, parent.valid() ? parent.traceId() : TraceContext::TraceId{},
            parent.valid() ? parent.spanId() : TraceContext::SpanId{}};
    {
        std::lock_guard lock(aggregator_->mutex_);
        auto& aggregate = aggregator_->open_[key];
        if (!aggregate) {
            aggregate = std::make_shared<Aggregate>(key, aggregator_->name_, aggregator_->kind_, parent);
        }
        aggregate_ = aggregate;
    }
    scope_.emplace(aggregate_->span.context().valid() ? aggregate_->span.context() : current());
}

SpanAggregator::Operation::~Operation() {
    if (!aggregate_) {
        return;
    }
    scope_.reset();

    const bool failed = failed_ || std::uncaught_exceptions() > uncaughtExceptions_;
    {
        std::lock_guard lock(aggregate_->mutex);
        ++aggregate_->calls;
        aggregate_->fields += fields_;
        aggregate_->bytes += bytes_;
        aggregate_->busy += std::chrono::steady_clock::now() - start_;
        aggregate_->lastEnd = std::chrono::system_clock::now();
        if (failed && !aggregate_->failed) {
            aggregate_->failed = true;
            aggregate_->span.setStatus(StatusCode::Error, failure_.empty() ? "exception" : failure_);
            for (auto& [key, value] : failureAttributes_) {
                aggregate_->span.setAttribute(std::move(key), std::move(value));
            }
        }
    }
    if (failed) {
        // the next operation starts a new aggregate; this one ends as soon as no operation uses it any more
        aggregator_->detach(aggregate_);
    }
    aggregate_.reset();
}

bool SpanAggregator::Operation::recording() const {
    return aggregate_ && aggregate_->span.recording();
}

void SpanAggregator::Operation::add(size_t fields, size_t bytes) {
    fields_ += fields;
    bytes_ += bytes;
}

void SpanAggregator::Operation::failed(std::string message, Attributes attributes) {
    if (!failed_) {
        failed_ = true;
        failure_ = std::move(message);
    }
    for (auto& attribute : attributes) {
        failureAttributes_.push_back(std::move(attribute));
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tracing
