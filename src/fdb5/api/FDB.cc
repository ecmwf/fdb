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

#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"

#include "metkit/codes/UserDataContent.h"
#include "metkit/hypercube/HyperCubePayloaded.h"
#include "metkit/mars/MarsExpension.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageDecoder.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDB::FDB(const Config &config) :
    internal_(FDBFactory::instance().build(config)),
    dirty_(false),
    reportStats_(config.getBool("statistics", false)) {}


FDB::~FDB() {
    flush();
    if (reportStats_ && internal_) {
        stats_.report(eckit::Log::info(), (internal_->name() + " ").c_str());
        internal_->stats().report(eckit::Log::info(), (internal_->name() + " internal ").c_str());
    }
}

void FDB::archive(eckit::message::Message msg) {
    fdb5::Key key = MessageDecoder::messageToKey(msg);
    archive(key, msg);
}

void FDB::archive(const Key& key, eckit::message::Message msg) {
    eckit::Timer timer;
    timer.start();

    internal_->archive(key, msg);
    dirty_ = true;

    timer.stop();
    stats_.addArchive(msg.length(), timer);
}

void FDB::archive(const Key& key, const void* data, size_t length) {
    eckit::message::Message msg{new metkit::codes::UserDataContent{data, length}};
    archive(key, msg);
}

bool FDB::sorted(const metkit::mars::MarsRequest &request) {

    bool sorted = false;

    const std::vector<std::string>& sort = request.values("optimise", /* emptyOK */ true);

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
        eckit::Log::userInfo() << "Using optimise" << std::endl;
    }

    eckit::Log::debug<LibFdb5>() << "fdb5::FDB::retrieve() Sorted? " << sorted << std::endl;

    return sorted;
}

class ListElementDeduplicator : public metkit::hypercube::Deduplicator<ListElement> {
public:
    bool toReplace(const ListElement& existing, const ListElement& replacement) const override {
        return existing.timestamp() < replacement.timestamp();
    }
};

eckit::DataHandle* FDB::retrieve(const metkit::mars::MarsRequest& request) {
    eckit::Timer timer;
    timer.start();

    HandleGatherer result(sorted(request));
    ListIterator it = inspect(request);
    ListElement el;

    if (it.next(el)) {
        // build the request representing the tensor-product of all retrieved fields
        metkit::mars::MarsRequest cubeRequest = el.combinedKey().request();
        std::vector<ListElement> elements{el};

        while (it.next(el)) {
            cubeRequest.merge(el.combinedKey().request());
            elements.push_back(el);
        }

        // checking all retrieved fields against the hypercube, to remove duplicates
        metkit::hypercube::HyperCubePayloaded<ListElement> cube(cubeRequest, ListElementDeduplicator());
        for(auto el: elements)
            cube.add(el.combinedKey().request(), el);

        if (cube.countVacant() > 0) {
            std::stringstream ss;
            ss << "No matching data for requests:" << std::endl;
            for (auto req: cube.vacantRequests()) {
                ss << "    " << req << std::endl;
            }
            eckit::Log::warning() << ss.str() << std::endl;
        }

        for (size_t i=0; i< cube.size(); i++) {
            ListElement element;
            if (cube.find(i, element)) {
                result.add(element.location().dataHandle());
            }
        }
    }
    return result.dataHandle();
}

ListIterator FDB::inspect(const metkit::mars::MarsRequest& request) {
    return internal_->inspect(request);
}

ListIterator FDB::list(const FDBToolRequest& request) {
    return internal_->list(request);
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

PurgeIterator FDB::purge(const FDBToolRequest &request, bool doit, bool porcelain) {
    return internal_->purge(request, doit, porcelain);
}

StatsIterator FDB::stats(const FDBToolRequest &request) {
    return internal_->stats(request);
}

ControlIterator FDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    return internal_->control(request, action, identifiers);
}

const std::string FDB::id() const {
    return internal_->id();
}

FDBStats FDB::stats() const {
    return stats_;
}

FDBStats FDB::internalStats() const {
    return internal_->stats();
}

const std::string& FDB::name() const {
    return internal_->name();
}

const Config &FDB::config() const {
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

bool FDB::dirty() const {
    return dirty_;
}

void FDB::disable() {
    internal_->disable();
}

bool FDB::disabled() const {
    return internal_->disabled();
}

bool FDB::writable() const {
    return internal_->writable();
}

bool FDB::visitable() const {
    return internal_->visitable();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
