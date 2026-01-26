/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/FDB.h"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/io/FieldHandle.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config& config) :
    internal_(FDBFactory::instance().build(config)), dirty_(false), reportStats_(config.getBool("statistics", false)) {
    LibFdb5::instance().constructorCallback()(*internal_);
}


FDB::~FDB() {
    flush();
    if (reportStats_ && internal_) {
        stats_.report(eckit::Log::info(), (internal_->name() + " ").c_str());
    }
}

FDB::FDB(FDB&&) = default;

FDB& FDB::operator=(FDB&&) = default;

void FDB::archive(eckit::message::Message msg) {
    fdb5::Key key = MessageDecoder::messageToKey(msg);
    archive(key, msg.data(), msg.length());
}
void FDB::archive(eckit::DataHandle& handle) {
    eckit::message::Message msg;
    eckit::message::Reader reader(handle);

    while ((msg = reader.next())) {
        archive(msg);
    }
}
void FDB::archive(const void* data, size_t length) {
    eckit::MemoryHandle handle(data, length);
    archive(handle);
}

void FDB::archive(const metkit::mars::MarsRequest& request, eckit::DataHandle& handle) {
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
        archive(key, msg.data(), msg.length());
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

void FDB::archive(const Key& key, const void* data, size_t length) {
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
    dirty_ = true;

    timer.stop();
    stats_.addArchive(length, timer);
}

void FDB::reindex(const Key& key, const FieldLocation& location) {
    internal_->reindex(key, location);
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

eckit::DataHandle* FDB::read(const eckit::URI& uri) {
    auto location = std::unique_ptr<FieldLocation>(FieldLocationFactory::instance().build(uri.scheme(), uri));
    return location->dataHandle();
}

eckit::DataHandle* FDB::read(const std::vector<eckit::URI>& uris, bool sorted) {
    HandleGatherer result(sorted);

    for (const eckit::URI& uri : uris) {
        auto location = std::unique_ptr<FieldLocation>(FieldLocationFactory::instance().build(uri.scheme(), uri));
        result.add(location->dataHandle());
    }

    return result.dataHandle();
}

eckit::DataHandle* FDB::read(ListIterator& it, bool sorted) {
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
                }
            }
        }
    }
    else {
        while (it.next(el)) {
            result.add(el.location().dataHandle());
        }
    }
    return result.dataHandle();
}

eckit::DataHandle* FDB::retrieve(const metkit::mars::MarsRequest& request) {
    static bool seekable = eckit::Resource<bool>("fdbSeekableDataHandle;$FDB_SEEKABLE_DATA_HANDLE", false);

    ListIterator it = inspect(request);
    return seekable ? new FieldHandle(it) : read(it, sorted(request));
}

ListIterator FDB::inspect(const metkit::mars::MarsRequest& request) {
    return internal_->inspect(request);
}

ListIterator FDB::list(const FDBToolRequest& request, const bool deduplicate, const int level, const bool onlyDuplicates) {
    return {internal_->list(request, level), deduplicate, onlyDuplicates};
}

DumpIterator FDB::dump(const FDBToolRequest& request, bool simple) {
    return internal_->dump(request, simple);
}

StatusIterator FDB::status(const FDBToolRequest& request) {
    return internal_->status(request);
}

WipeIterator FDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    return internal_->wipe(request, doit, porcelain, unsafeWipeAll);
}

PurgeIterator FDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    return internal_->purge(request, doit, porcelain);
}

StatsIterator FDB::stats(const FDBToolRequest& request) {
    return internal_->stats(request);
}

ControlIterator FDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    return internal_->control(request, action, identifiers);
}

const std::string FDB::id() const {
    return internal_->id();
}

MoveIterator FDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    return internal_->move(request, dest);
}

FDBStats FDB::stats() const {
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

void FDB::flush() {
    if (dirty_) {
        eckit::Timer timer;
        timer.start();

        internal_->flush();
        dirty_ = false;

        timer.stop();
        stats_.addFlush(timer);
    }
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
