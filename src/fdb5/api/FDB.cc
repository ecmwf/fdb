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

#include "eckit/config/Resource.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include "metkit/codes/UserDataContent.h"
#include "metkit/hypercube/HyperCubePayloaded.h"

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
    archive(key, msg.data(), msg.length());
}
void FDB::archive(eckit::DataHandle& handle) {
    eckit::message::Message msg;
    eckit::message::Reader reader(handle);

    while ( (msg = reader.next()) ) {
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

    while ( (msg = reader.next()) ) {
        fdb5::Key key = MessageDecoder::messageToKey(msg);
        if (!cube.clear(key.request())) {
            std::stringstream ss;
            ss << "FDB archive - found unexpected message" << std::endl;
            ss << "  user request:"  << std::endl << "    " << request << std::endl;
            ss << "  unexpected message:" << std::endl << "    " << key << std::endl;
            eckit::Log::debug<LibFdb5>() << ss.str();
            throw eckit::UserError(ss.str(), Here());
        }
        archive(key, msg.data(), msg.length());
    }
    if (cube.countVacant()) {
        std::stringstream ss;
        ss << "FDB archive - missing " << cube.countVacant() << " messages" << std::endl;
        ss << "  user request:"  << std::endl << "    " << request << std::endl;
        ss << "  missing messages:" << std::endl;
        for (auto vacantRequest : cube.vacantRequests()) {
            ss << "    " << vacantRequest << std::endl;
        }
        eckit::Log::debug<LibFdb5>() << ss.str();
        throw eckit::UserError(ss.str(), Here());
    }
}

void FDB::archive(const Key& key, const void* data, size_t length) {
    eckit::Timer timer;
    timer.start();

    internal_->archive(key, data, length);
    dirty_ = true;

    timer.stop();
    stats_.addArchive(length, timer);
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
            ListElementDeduplicator dedup;
            metkit::hypercube::HyperCubePayloaded<ListElement> cube(cubeRequest, dedup);
            for(auto el: elements) {
                cube.add(el.combinedKey().request(), el);
            }

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
    }
    else {
        while (it.next(el)) {
            result.add(el.location().dataHandle());
        }
    }
    return result.dataHandle();
}

ListIterator FDB::inspect(const metkit::mars::MarsRequest& request) {
    return internal_->inspect(request);
}

ListIterator FDB::list(const FDBToolRequest& request, bool deduplicate) {
    return ListIterator(internal_->list(request), deduplicate);
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

void FDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    if (request.all()) {
        std::stringstream ss;
        ss << "Move ALL not supported. Please specify a single database." << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    // check that the request is only referring a single DB - no ranges of values
    const metkit::mars::MarsRequest& req = request.request();
    std::vector<std::string> params = req.params();
    for (const std::string& param: params) {
        const std::vector<std::string>& values = req.values(param);

        if (values.size() != 1) {
            std::stringstream ss;
            ss << "Move requires a single value for each parameter in the request." << std::endl << "Parameter " << param << "=" << values << " not supported." << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
    }

    StatsIterator it = stats(request);
    StatsElement se;
    if (!it.next(se)) {
        std::stringstream ss;
        ss << "Request " << req << " does not matches with an existing database. Please specify a single database." << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }
    if (it.next(se)) {
        std::stringstream ss;
        ss << "Request " << req << " matches with more than one existing database. Please specify a single database." << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    // lock the database (and check it is locked)
    ControlIterator controlIter = control(request, ControlAction::Disable, ControlIdentifier::Archive | ControlIdentifier::Wipe | ControlIdentifier::UniqueRoot);
    
    ControlElement elem;
    bool locked = controlIter.next(elem);
    locked = locked && !elem.controlIdentifiers.enabled(ControlIdentifier::Archive);
    locked = locked && !elem.controlIdentifiers.enabled(ControlIdentifier::Wipe);
    locked = locked && !elem.controlIdentifiers.enabled(ControlIdentifier::UniqueRoot);
    if (!locked) {
        std::stringstream ss;
        ss << "Source DB cannot be locked for moving" << std::endl;
        throw eckit::UserError(ss.str(), Here());

    }

    // check that reqest and DB key are matching (request is not more specific)
    ASSERT(req.matches(elem.key.request()));
    ASSERT(elem.key.request().matches(req));

    internal_->move(request, dest);
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

bool FDB::enabled(const ControlIdentifier& controlIdentifier) const {
    return internal_->enabled(controlIdentifier);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
