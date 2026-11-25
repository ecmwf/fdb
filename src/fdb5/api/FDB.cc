// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/FDB.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/FDBStats.h"
#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeCoordinator.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/io/FieldHandle.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"
#include "fdb5/types/Type.h"

#include "metkit/hypercube/HyperCube.h"
#include "metkit/hypercube/HyperCubePayloaded.h"

#include "eckit/config/Resource.h"
#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include <cctype>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

namespace {

constexpr size_t maxRequestAttributeLength = 1024;

template <typename T>
std::string toAttributeString(const T& value) {
    std::ostringstream ss;
    ss << value;
    std::string s = ss.str();
    if (s.size() > maxRequestAttributeLength) {
        s.resize(maxRequestAttributeLength);
        s += "...";
    }
    return s;
}

/// The span of one FDB API call. It is the current context of the calling thread while in scope, so that work
/// done on behalf of the call (including remote requests) is attributed to it. A no-op if tracing is disabled.
class ApiSpan {
public:

    /// Child of @p parent if valid, otherwise of the calling thread's current context (or a new root)
    ApiSpan(const char* name, const tracing::TraceContext& parent) :
        span_(name, tracing::SpanKind::Internal, parent.valid() ? parent : tracing::current()) {}

    bool recording() { return span_.span().recording(); }

    template <typename T>
    void setAttribute(const char* key, T&& value) {
        span_.setAttribute(key, std::forward<T>(value));
    }

private:

    tracing::ScopedSpan span_;
};

/// For API calls returning an iterator: the span is current while the iterator is created (so that asynchronous
/// workers and remote requests inherit it) and is then handed over to the iterator, ending with the iteration.
class IteratorSpan {
public:

    IteratorSpan(const char* name, const tracing::TraceContext& explicitParent) :
        span_(name, tracing::SpanKind::Internal, explicitParent.valid() ? explicitParent : tracing::current()) {
        scope_.emplace(span_.context().valid() ? span_.context() : tracing::current());
    }

    template <typename T>
    void setAttribute(const char* key, T&& value) {
        span_.setAttribute(key, std::forward<T>(value));
    }

    bool recording() const { return span_.recording(); }

    template <typename Iterator>
    Iterator attach(Iterator&& it) {
        scope_.reset();
        it.attachSpan(std::move(span_));
        return std::move(it);
    }

private:

    tracing::Span span_;
    std::optional<tracing::ScopedContext> scope_;
};

}  // namespace

/// An archive call, traced as part of the aggregated archive span of its flush cycle and parent context
class FDB::ArchiveOperation : public tracing::SpanAggregator::Operation {
public:

    /// An archive call of @p fdb, with the caller's context @p trace (see archiveParent). Untraced if tracing is
    /// disabled.
    ArchiveOperation(FDB& fdb, const tracing::TraceContext& trace) : ArchiveOperation(fdb, trace, fdb.archiveSpans()) {}

    /// Archive one field, recording it in the span (and its key, if it fails)
    template <typename F>
    void archive(const Key& key, size_t length, F&& doArchive) {
        begin();  // from now on, a flush may cover the field, before it is recorded
        try {
            doArchive();
        }
        catch (const std::exception& e) {
            if (recording()) {
                failed(e.what(), {{"fdb.key", toAttributeString(key)}});
            }
            throw;
        }
        add(1, length);
    }

private:

    ArchiveOperation(FDB& fdb, const tracing::TraceContext& trace, tracing::SpanAggregator* spans) :
        ArchiveOperation(fdb, trace, spans,
                         spans ? std::unique_lock<std::mutex>(fdb.cycleMutex_) : std::unique_lock<std::mutex>()) {}

    /// n.b. the parent is chosen, and the operation registered in its aggregate, with @p lock (FDB::cycleMutex_) held: the
    /// lock is released once the delegating constructor completes. A concurrent flush thus closes the archive cycle,
    /// and ends its aggregates, either before the parent is chosen (the operation is in the next cycle) or after the
    /// operation is registered (its aggregate, kept open by the operation, ends when it is over). It never ends a cycle
    /// that an archive is about to join. The archival itself is not under the lock, and neither is FDB::mutex_ (held
    /// by a flush for its whole duration): archive calls do not wait for a flush in progress.
    ArchiveOperation(FDB& fdb, const tracing::TraceContext& trace, tracing::SpanAggregator* spans,
                     std::unique_lock<std::mutex>&& /* lock */) :
        Operation(spans, spans ? fdb.archiveParent(trace) : tracing::TraceContext{}) {}
};

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Build the implementation of an FDB. The tracing settings of the configuration apply first: connecting to a remote
/// FDB negotiates trace-context propagation, which depends on them.
std::shared_ptr<FDBBase> buildInternal(const Config& config) {
    tracing::Tracer::instance().configureFrom(config);
    return FDBFactory::instance().build(config);
}

}  // namespace

FDB::FDB(const Config& config) :
    internal_(buildInternal(config)),
    dirty_(false),
    reportStats_(config.getBool("statistics", false)),
    archiveSpans_(std::make_unique<tracing::SpanAggregator>("fdb.archive", tracing::SpanKind::Internal)) {
    // n.b. the first tracing settings found in the process apply: those supplied (see buildInternal) take precedence
    tracing::Tracer::instance().configureFrom(internal_->config());
    LibFdb5::instance().constructorCallback()(*internal_);
}


FDB::~FDB() {
    try {
        flush();
    }
    catch (const std::exception& e) {
        eckit::Log::error() << "FDB flush failed during destruction: " << e.what() << std::endl;
    }
    catch (...) {
        eckit::Log::error() << "FDB flush failed during destruction with an unknown exception" << std::endl;
    }
    if (reportStats_ && internal_) {
        stats_.report(eckit::Log::info(), (internal_->name() + " ").c_str());
    }
}

/// @note Moves are not thread-safe with respect to concurrent use of the source
FDB::FDB(FDB&& rhs) noexcept :
    internal_(std::move(rhs.internal_)),
    dirty_(rhs.dirty_),
    reportStats_(rhs.reportStats_),
    archiveCycle_(std::move(rhs.archiveCycle_)),
    archiveSpans_(std::move(rhs.archiveSpans_)),
    stats_(std::move(rhs.stats_)) {
    // clear the dirty flag so its destructor does not flush its now null internal_
    rhs.dirty_ = false;
}

FDB& FDB::operator=(FDB&& rhs) noexcept {
    if (this != &rhs) {
        internal_ = std::move(rhs.internal_);
        dirty_ = rhs.dirty_;
        reportStats_ = rhs.reportStats_;
        archiveCycle_ = std::move(rhs.archiveCycle_);
        archiveSpans_ = std::move(rhs.archiveSpans_);
        stats_ = std::move(rhs.stats_);
        rhs.dirty_ = false;
    }
    return *this;
}

tracing::SpanAggregator* FDB::archiveSpans() const {
    return tracing::Tracer::instance().enabled() ? archiveSpans_.get() : nullptr;
}

tracing::TraceContext FDB::archiveParent(const tracing::TraceContext& trace) {
    if (trace.valid()) {
        return trace;
    }
    if (tracing::current().valid()) {
        return tracing::current();
    }
    if (!archiveCycle_) {
        archiveCycle_ =
            std::make_unique<tracing::Span>("fdb.archive_cycle", tracing::SpanKind::Internal, tracing::TraceContext{});
        // ended by flush(), possibly while unwinding for an unrelated reason (~FDB): not a failure of the cycle
        archiveCycle_->detectUnwinding(false);
    }
    return archiveCycle_->context();
}

void FDB::archive(eckit::message::Message msg, const tracing::TraceContext& trace) {
    ArchiveOperation op(*this, trace);
    fdb5::Key key = MessageDecoder::messageToKey(msg);
    op.archive(key, msg.length(), [&] { archiveImpl(key, msg.data(), msg.length()); });
}

void FDB::archive(eckit::DataHandle& handle, const tracing::TraceContext& trace) {
    ArchiveOperation op(*this, trace);
    eckit::message::Message msg;
    eckit::message::Reader reader(handle);

    while ((msg = reader.next())) {
        fdb5::Key key = MessageDecoder::messageToKey(msg);
        op.archive(key, msg.length(), [&] { archiveImpl(key, msg.data(), msg.length()); });
    }
}

void FDB::archive(const void* data, size_t length, const tracing::TraceContext& trace) {
    eckit::MemoryHandle handle(data, length);
    archive(handle, trace);
}

void FDB::archive(const metkit::mars::MarsRequest& request, eckit::DataHandle& handle,
                  const tracing::TraceContext& trace) {
    ArchiveOperation op(*this, trace);
    try {
        archiveRequest(op, request, handle);
    }
    catch (const std::exception& e) {
        if (op.recording()) {
            op.failed(e.what(), {{"fdb.request", toAttributeString(request)}});
        }
        throw;
    }
}

void FDB::archiveRequest(ArchiveOperation& op, const metkit::mars::MarsRequest& request, eckit::DataHandle& handle) {
    eckit::message::Message msg;
    eckit::message::Reader reader(handle);

    metkit::hypercube::HyperCube cube(request);

    while ((msg = reader.next())) {
        fdb5::Key key = MessageDecoder::messageToKey(msg);
        if (!cube.clear(key.request())) {
            std::ostringstream ss;
            ss << "FDB archive - found unexpected message" << std::endl;
            ss << "  user request:" << std::endl << "    " << request << std::endl;
            ss << "  unexpected message:" << std::endl << "    " << key << std::endl;
            LOG_DEBUG_LIB(LibFdb5) << ss.str();
            throw eckit::UserError(ss.str(), Here());
        }
        op.archive(key, msg.length(), [&] { archiveImpl(key, msg.data(), msg.length()); });
    }
    if (cube.countVacant()) {
        std::ostringstream ss;
        ss << "FDB archive - missing " << cube.countVacant() << " messages" << std::endl;
        ss << "  user request:" << std::endl << "    " << request << std::endl;
        ss << "  missing messages:" << std::endl;
        for (auto vacantRequest : cube.vacantRequests()) {
            ss << "    " << vacantRequest << std::endl;
        }
        LOG_DEBUG_LIB(LibFdb5) << ss.str();
        throw eckit::UserError(ss.str(), Here());
    }
}

void FDB::archive(const Key& key, const void* data, size_t length, const tracing::TraceContext& trace) {
    ArchiveOperation op(*this, trace);
    op.archive(key, length, [&] { archiveImpl(key, data, length); });
}

void FDB::archiveImpl(const Key& key, const void* data, size_t length) {
    eckit::Timer timer;
    timer.start();

    // This is the API entrypoint. Keys supplied by the user may not have type registry info attached (so
    // serialisation won't work properly...)
    Key keyInternal(key);

    // step in archival requests from the model is just an integer. We need to include the stepunit
    if (const auto [stepunit, found] = keyInternal.find("stepunits"); found) {
        if (stepunit->second.size() > 0 && static_cast<char>(tolower(stepunit->second[0])) != 'h') {
            if (auto [step, foundStep] = keyInternal.find("step"); foundStep) {
                std::string canonicalStep = config().schema().registry().lookupType("step").toKey(
                    step->second + static_cast<char>(tolower(stepunit->second[0])));
                keyInternal.set("step", canonicalStep);
            }
        }
        keyInternal.unset("stepunits");
    }

    internal_->archive(keyInternal, data, length);

    timer.stop();

    std::lock_guard lock(mutex_);
    dirty_ = true;
    stats_.addArchive(length, timer);
}

void FDB::reindex(const Key& key, const FieldLocation& location) {
    internal_->reindex(key, location);
    std::lock_guard lock(mutex_);
    dirty_ = true;
}

bool FDB::sorted(const metkit::mars::MarsRequest& request) {

    bool sorted = false;

    const std::vector<std::string>& sort = request.values("optimise", /* emptyOK */ true);

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
        eckit::Log::userInfo() << "Using optimise" << std::endl;
    }

    LOG_DEBUG_LIB(LibFdb5) << "fdb5::FDB::retrieve() Sorted? " << sorted << std::endl;

    return sorted;
}

eckit::DataHandle* FDB::read(const eckit::URI& uri, const tracing::TraceContext& trace) {
    ApiSpan span("fdb.read", trace);
    span.setAttribute("fdb.fields", 1);
    auto location = std::unique_ptr<FieldLocation>(FieldLocationFactory::instance().build(uri.scheme(), uri));
    return location->dataHandle();
}

eckit::DataHandle* FDB::read(const std::vector<eckit::URI>& uris, bool sorted, const tracing::TraceContext& trace) {
    ApiSpan span("fdb.read", trace);
    span.setAttribute("fdb.fields", uris.size());
    HandleGatherer result(sorted);

    for (const eckit::URI& uri : uris) {
        auto location = std::unique_ptr<FieldLocation>(FieldLocationFactory::instance().build(uri.scheme(), uri));
        result.add(location->dataHandle());
    }

    return result.dataHandle();
}

eckit::DataHandle* FDB::read(ListIterator& it, bool sorted, const tracing::TraceContext& trace) {
    ApiSpan span("fdb.read", trace);
    size_t fields = 0;

    eckit::Timer timer;
    timer.start();

    HandleGatherer result(sorted);
    ListElement el;

    static bool dedup = eckit::Resource<bool>("fdbDeduplicate;$FDB_DEDUPLICATE_FIELDS", false);
    if (dedup) {
        if (it.next(el)) {
            // build the request representing the tensor-product of all retrieved fields
            metkit::mars::MarsRequest cubeRequest = el.combinedKey().request();
            std::vector<ListElement> elements{el};

            while (it.next(el)) {
                cubeRequest.merge(el.combinedKey().request());
                elements.push_back(el);
            }

            // checking all retrieved fields against the hypercube, to remove duplicates
            ListElementDeduplicator deduplicator;
            metkit::hypercube::HyperCubePayloaded<ListElement> cube(cubeRequest, deduplicator);
            for (const auto& elem : elements) {
                cube.add(elem.combinedKey().request(), el);
            }

            if (cube.countVacant() > 0) {
                std::ostringstream ss;
                ss << "No matching data for requests:" << std::endl;
                for (auto req : cube.vacantRequests()) {
                    ss << "    " << req << std::endl;
                }
                eckit::Log::warning() << ss.str() << std::endl;
            }

            for (std::size_t i = 0; i < cube.size(); i++) {
                ListElement element;
                if (cube.find(i, element)) {
                    result.add(element.location().dataHandle());
                    ++fields;
                }
            }
        }
    }
    else {
        while (it.next(el)) {
            result.add(el.location().dataHandle());
            ++fields;
        }
    }
    span.setAttribute("fdb.fields", fields);
    return result.dataHandle();
}

eckit::DataHandle* FDB::retrieve(const metkit::mars::MarsRequest& request, const tracing::TraceContext& trace) {
    static bool seekable = eckit::Resource<bool>("fdbSeekableDataHandle;$FDB_SEEKABLE_DATA_HANDLE", false);

    ApiSpan span("fdb.retrieve", trace);
    if (span.recording()) {
        span.setAttribute("fdb.request", toAttributeString(request));
    }

    // inspect() and read() record child spans of this one
    ListIterator it = inspect(request);
    return seekable ? new FieldHandle(it) : read(it, sorted(request));
}

ListIterator FDB::inspect(const metkit::mars::MarsRequest& request, const tracing::TraceContext& trace) {
    IteratorSpan span("fdb.inspect", trace);
    if (span.recording()) {
        span.setAttribute("fdb.request", toAttributeString(request));
    }
    return span.attach(internal_->inspect(request));
}

ListIterator FDB::list(const FDBToolRequest& request, const ListMode mode, const int level,
                       const tracing::TraceContext& trace) {
    IteratorSpan span("fdb.list", trace);
    if (span.recording()) {
        span.setAttribute("fdb.request", toAttributeString(request.request()));
    }
    span.setAttribute("fdb.level", level);
    return span.attach(ListIterator{internal_->list(request, level), mode});
}

ListIterator FDB::list(const FDBToolRequest& request, const bool deduplicate, const int level,
                       const tracing::TraceContext& trace) {
    return list(request, deduplicate ? ListMode::Deduplicate : ListMode::Full, level, trace);
}

DumpIterator FDB::dump(const FDBToolRequest& request, bool simple) {
    return internal_->dump(request, simple);
}

StatusIterator FDB::touch(const FDBToolRequest& request, AccessIdentifiers identifiers, AccessTime when) {
    return internal_->touch(request, identifiers, when);
}

StatusIterator FDB::status(const FDBToolRequest& request) {
    return internal_->status(request);
}

WipeIterator FDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll,
                       const tracing::TraceContext& trace) {

    IteratorSpan span("fdb.wipe", trace);
    if (span.recording()) {
        span.setAttribute("fdb.request", toAttributeString(request.request()));
    }
    span.setAttribute("fdb.doit", doit);
    span.setAttribute("fdb.unsafe_wipe_all", unsafeWipeAll);

    auto internal = internal_->shared();

    auto async = [internal, request, doit, porcelain, unsafeWipeAll](eckit::Queue<WipeElement>& queue) {
        // Visit the catalogues to determine what they would wipe
        WipeStateIterator it = internal->wipe(request, doit, porcelain, unsafeWipeAll);

        // Coordinate the wipe across catalogues and stores
        WipeCoordinator coordinator;
        CatalogueWipeState catalogueWipeState{Key(), internal->config()};
        while (it.next(catalogueWipeState)) {

            auto elements = coordinator.wipe(catalogueWipeState, doit, unsafeWipeAll);
            for (auto& el : elements) {
                queue.emplace(el);
            }
        }
    };

    return span.attach(
        WipeIterator(new APIAsyncIterator<WipeElement>(internal_->shared(), async, internal_->config())));
}

PurgeIterator FDB::purge(const FDBToolRequest& request, bool doit, bool porcelain, const tracing::TraceContext& trace) {
    IteratorSpan span("fdb.purge", trace);
    if (span.recording()) {
        span.setAttribute("fdb.request", toAttributeString(request.request()));
    }
    span.setAttribute("fdb.doit", doit);
    return span.attach(internal_->purge(request, doit, porcelain));
}

StatsIterator FDB::stats(const FDBToolRequest& request) {
    return internal_->stats(request);
}

ControlIterator FDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    return internal_->control(request, action, identifiers);
}

MoveIterator FDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    return internal_->move(request, dest);
}

FDBStats FDB::stats() const {
    std::lock_guard lock(mutex_);
    return stats_;
}

const std::string& FDB::name() const {
    return internal_->name();
}

const Config& FDB::config() const {
    return internal_->config();
}

void FDB::print(std::ostream& s) const {
    s << *internal_;
}

void FDB::flush(const tracing::TraceContext& trace) {
    std::lock_guard lock(mutex_);

    // The archive spans of the cycle are taken, and end once the flush is over. If it fails, their traces are exported
    // with it: they may be other traces than the flush (e.g. archives with an explicit context), which would
    // otherwise be complete, and not exported. Their end time is that of their last archive.
    // The aggregates of the archives still in progress (in other threads) stay open: the next flush takes them, but
    // their traces are exported too if this one fails (they may have written data that it makes durable). So does
    // the archive-cycle span the aggregates are children of: it is ended once all its archives are, after the flush
    // span, so that the next flush is in the same trace. The aggregates of other parents (explicit contexts) do not
    // keep it open. Every archive which joined the cycle is registered in its aggregate (see ArchiveOperation): later
    // archives start the next cycle.
    // A flush with nothing to flush ends the spans which are over all the same, but covers nothing: the fields being
    // written are left for the flush which covers them.
    // n.b. cycleMutex_ is held until then, not during the flush itself. There are no archive spans in a moved-from FDB
    std::unique_lock cycleLock(cycleMutex_);
    std::unique_ptr<tracing::Span> cycle;
    tracing::SpanAggregator::Detached archives;
    if (archiveSpans_) {
        archives = archiveSpans_->detachIdle(dirty_);
    }
    // n.b. the context of the cycle, which may stay open (see above)
    const tracing::TraceContext cycleContext = archiveCycle_ ? archiveCycle_->context() : tracing::TraceContext{};
    if (!archiveSpans_ || !cycleContext.valid() || !archiveSpans_->usesParent(cycleContext)) {
        cycle = std::move(archiveCycle_);
    }
    cycleLock.unlock();

    if (!dirty_) {
        return;
    }

    // explicit context, else the current one, else the archive cycle being flushed
    const tracing::TraceContext& parent =
        (trace.valid() || tracing::current().valid() || !cycleContext.valid()) ? trace : cycleContext;
    ApiSpan span("fdb.flush", parent);

    eckit::Timer timer;
    timer.start();

    try {
        internal_->flush();
    }
    catch (...) {
        // The archives which began after the spans were taken, and before the data was flushed, may be covered; not
        // those which begin from now on. n.b. the first thing: it is the position of the failure that counts.
        // This is the earliest that FDB::flush can tell: the backend throws from its own frames, which unwind before
        // this handler runs. An archive which begins during that time is attributed to the failure too (an extra
        // trace exported, nothing lost). Making it exact needs the backends to report where they failed.
        const uint64_t failedAt = archiveSpans_ ? archiveSpans_->mark() : 0;
        archives.retainTraces();
        if (archiveSpans_) {
            archiveSpans_->retainTracesOfNewFields(failedAt);
        }
        throw;
    }
    archives.flushed();
    dirty_ = false;

    timer.stop();
    stats_.addFlush(timer);
}

IndexAxis FDB::axes(const FDBToolRequest& request, int level) {
    IndexAxis axes;
    AxesElement elem;
    auto it = axesIterator(request, level);
    while (it.next(elem)) {
        axes.merge(elem.axes());
    }
    return axes;
}

AxesIterator FDB::axesIterator(const FDBToolRequest& request, int level) {
    return internal_->axesIterator(request, level);
}

bool FDB::dirty() const {
    std::lock_guard lock(mutex_);
    return dirty_;
}

bool FDB::enabled(const ControlIdentifier& controlIdentifier) const {
    return internal_->enabled(controlIdentifier);
}

void FDB::registerArchiveCallback(ArchiveCallback callback) {
    internal_->registerArchiveCallback(callback);
}

void FDB::registerFlushCallback(FlushCallback callback) {
    internal_->registerFlushCallback(callback);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
