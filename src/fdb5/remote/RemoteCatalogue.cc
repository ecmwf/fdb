/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/FdbEndpoint.h"
#include "fdb5/remote/RemoteCatalogue.h"
#include "fdb5/remote/RemoteEngine.h"

#include <unordered_map>

using namespace eckit;
namespace fdb5::remote {

class CatalogueArchivalRequest {

public:

    CatalogueArchivalRequest() :
        id_(0), catalogue_(nullptr), key_(Key()) {}

    CatalogueArchivalRequest(uint32_t id, RemoteCatalogue* catalogue, const fdb5::Key& key, std::unique_ptr<FieldLocation> location) :
        id_(id), catalogue_(catalogue), key_(key), location_(std::move(location)) {}

    std::pair<uint32_t, std::pair<Key, std::unique_ptr<FieldLocation>>>  element;

    uint32_t id_;
    RemoteCatalogue* catalogue_;
    fdb5::Key key_;
    std::unique_ptr<FieldLocation> location_;
};


class RemoteCatalogueArchiver {

public: // types

    using CatalogueArchiveQueue = eckit::Queue<CatalogueArchivalRequest>;

public: // methods

    static RemoteCatalogueArchiver* get(uint64_t archiverID);

    bool valid() { return archiveFuture_.valid(); }
    bool dirty() { return dirty_; }

    void start();
    void error(eckit::Buffer payload, const eckit::net::Endpoint& endpoint);
    
    void emplace(uint32_t id, RemoteCatalogue* catalogue, const Key& key, std::unique_ptr<FieldLocation> location);
    FDBStats flush(RemoteCatalogue* catalogue);

private: // methods

    RemoteCatalogueArchiver() : dirty_(false), maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200)) {}

    FDBStats archiveThreadLoop();

private: // members

    bool dirty_;

    std::mutex archiveQueuePtrMutex_;
    size_t maxArchiveQueueLength_;
    std::unique_ptr<CatalogueArchiveQueue> archiveQueue_;
    std::future<FDBStats> archiveFuture_;

};

RemoteCatalogueArchiver* RemoteCatalogueArchiver::get(uint64_t archiverID) {

    static std::unordered_map<uint64_t, std::unique_ptr<RemoteCatalogueArchiver>> archivers_;
    static std::mutex getArchiverMutex_;

    std::lock_guard<std::mutex> lock(getArchiverMutex_);

    auto it = archivers_.find(archiverID);
    if (it == archivers_.end()) {
        it = archivers_.emplace(archiverID, new RemoteCatalogueArchiver()).first;
    }
    return it->second.get();
}

void RemoteCatalogueArchiver::start() {
    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiveFuture_.valid()) {

        {
            // Reset the queue after previous done/errors
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            ASSERT(!archiveQueue_);
            archiveQueue_.reset(new CatalogueArchiveQueue(maxArchiveQueueLength_));
        }

        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }
}

void RemoteCatalogueArchiver::error(eckit::Buffer payload, const eckit::net::Endpoint& endpoint) {

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);

    if (archiveQueue_) {
        std::string msg;
        if (payload.size() > 0) {
            msg.resize(payload.size(), ' ');
            payload.copy(&msg[0], payload.size());
        }

        archiveQueue_->interrupt(std::make_exception_ptr(RemoteFDBException(msg, endpoint)));
    }
}

void RemoteCatalogueArchiver::emplace(uint32_t id, RemoteCatalogue* catalogue, const Key& key, std::unique_ptr<FieldLocation> location) {

    dirty_ = true;

    ASSERT(archiveQueue_);
    archiveQueue_->emplace(id, catalogue, key, std::move(location));
}

FDBStats RemoteCatalogueArchiver::flush(RemoteCatalogue* catalogue) {

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
    ASSERT(archiveQueue_);
    archiveQueue_->close();

    FDBStats stats = archiveFuture_.get();
    ASSERT(!archiveQueue_);

    ASSERT(stats.numFlush() == 0);
    size_t numArchive = stats.numArchive();

    Buffer sendBuf(4096);
    MemoryStream s(sendBuf);
    s << numArchive;

    eckit::Log::debug<LibFdb5>() << " RemoteCatalogue::flush - flushing " << numArchive << " fields" << std::endl;
    uint32_t id = catalogue->generateRequestID();
    // The flush call is blocking
    catalogue->controlWriteCheckResponse(Message::Flush, id, sendBuf, s.position());

    dirty_ = false;

    return stats;
}

FDBStats RemoteCatalogueArchiver::archiveThreadLoop() {
    FDBStats localStats;
    eckit::Timer timer;

    CatalogueArchivalRequest element;

    try {

        ASSERT(archiveQueue_);
        long popped;
        while ((popped = archiveQueue_->pop(element)) != -1) {

            timer.start();
            element.catalogue_->sendArchiveData(element.id_, element.key_, std::move(element.location_));
            timer.stop();

            localStats.addArchive(0, timer, 1);
        }

        // And note that we are done. (don't time this, as already being blocked
        // on by the ::flush() routine)

        element.catalogue_->dataWrite(Message::Flush, 0);

        archiveQueue_.reset();

    } catch (...) {
        archiveQueue_->interrupt(std::current_exception());
        throw;
    }

    return localStats;

    // We are inside an async, so don't need to worry about exceptions escaping.
    // They will be released when flush() is called.
}


std::string host(const eckit::Configuration& config) {
    std::string host = config.getString("host");
    std::string remoteDomain = config.getString("remoteDomain", "");
    if (remoteDomain.empty()) {
        return host;
    }
    return host+remoteDomain;
}


RemoteCatalogue::RemoteCatalogue(const Key& key, const Config& config):
    CatalogueImpl(key, ControlIdentifiers(), config), // xxx what are control identifiers? Setting empty here...
    Client(eckit::net::Endpoint(host(config), config.getInt("port"))),
    config_(config), schema_(nullptr), archiver_(nullptr) {

    loadSchema();
}

RemoteCatalogue::RemoteCatalogue(const eckit::URI& uri, const Config& config):
    Client(eckit::net::Endpoint(host(config), config.getInt("port"))), config_(config), schema_(nullptr), archiver_(nullptr)
    {
        NOTIMP;
    }


void RemoteCatalogue::sendArchiveData(uint32_t id, const Key& key, std::unique_ptr<FieldLocation> fieldLocation)
{
    ASSERT(!dbKey_.empty());
    ASSERT(!currentIndexKey_.empty());
    ASSERT(!key.empty());
    ASSERT(fieldLocation);

    Buffer keyBuffer(4096);
    MemoryStream keyStream(keyBuffer);
//    keyStream << dbKey_;
    keyStream << currentIndexKey_;
    keyStream << key;

    Buffer locBuffer(4096);
    MemoryStream locStream(locBuffer);
    locStream << *fieldLocation;

    std::vector<std::pair<const void*, uint32_t>> payloads;
    payloads.push_back(std::pair<const void*, uint32_t>{keyBuffer, keyStream.position()});
    payloads.push_back(std::pair<const void*, uint32_t>{locBuffer, locStream.position()});

    dataWrite(Message::Blob, id, payloads);
}

void RemoteCatalogue::archive(const uint32_t archiverID, const InspectionKey& key, std::unique_ptr<FieldLocation> fieldLocation) {

    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiver_) {
        uint64_t archiverName = std::hash<FdbEndpoint>()(controlEndpoint());
        archiverName = archiverName << 32;
        archiverName += archiverID;
        archiver_ = RemoteCatalogueArchiver::get(archiverName);
    }
    uint32_t id = connection_.generateRequestID();
    if (!archiver_->valid()) {
        archiver_->start();
        ASSERT(archiver_->valid());

        // std::cout << "Catalogue - controlWriteCheckResponse(Message::Archive)" << std::endl;
        controlWriteCheckResponse(Message::Archive, id);
    }
    // eckit::Log::debug<LibFdb5>() << " RemoteCatalogue::archive - adding to queue [id=" << id << ",key=" << key << ",fieldLocation=" << fieldLocation->uri() << "]" << std::endl;
    archiver_->emplace(id, this, key, std::move(fieldLocation));
}

bool RemoteCatalogue::selectIndex(const Key& idxKey) {
    currentIndexKey_ = idxKey;
    return true; // xxx whats the return used for? TOC always returns true
}

const Index& RemoteCatalogue::currentIndex(){
    return nullptr;
}
const Key RemoteCatalogue::currentIndexKey() {
    return currentIndexKey_;
}

void RemoteCatalogue::deselectIndex() {
    currentIndexKey_ = Key();
}
const Schema& RemoteCatalogue::schema() const {
    ASSERT(schema_);
    return *schema_;
}

void RemoteCatalogue::flush() {

    Timer timer;

    timer.start();

    // Flush only does anything if there is an ongoing archive();
    if (archiver_->valid()) {
        archiver_->flush(this);
//        internalStats_ += stats;
    }

    timer.stop();
//    internalStats_.addFlush(timer);
}

void RemoteCatalogue::clean() {NOTIMP;}

void RemoteCatalogue::close() {NOTIMP;}

bool RemoteCatalogue::exists() const {NOTIMP;}

void RemoteCatalogue::checkUID() const {}

eckit::URI RemoteCatalogue::uri() const {
    return eckit::URI(/*RemoteEngine::typeName()*/ "fdb", controlEndpoint().host(), controlEndpoint().port());
    // return eckit::URI(TocEngine::typeName(), basePath());
}

void RemoteCatalogue::loadSchema() {
    // NB we're at the db level, so get the db schema. We will want to get the master schema beforehand.
    // (outside of the catalogue) 

    if (!schema_) {
        eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::loadSchema()" << std::endl;

        // send dbkey to remote.
        eckit::Buffer keyBuffer(4096);
        eckit::MemoryStream keyStream(keyBuffer);
        keyStream << dbKey_;
        
        uint32_t id = generateRequestID();
        eckit::Buffer buf = controlWriteReadResponse(Message::Schema, id, keyBuffer, keyStream.position());

        eckit::MemoryStream s(buf);
        schema_.reset(eckit::Reanimator<fdb5::Schema>::reanimate(s));
    }
}

bool RemoteCatalogue::handle(Message message, uint32_t requestID) {
    eckit::Log::debug<LibFdb5>() << *this << " - Received [message=" << ((uint) message) << ",requestID=" << requestID << "]" << std::endl;
    NOTIMP;
    return false;
}
bool RemoteCatalogue::handle(Message message, uint32_t requestID, eckit::Buffer&& payload) {
    eckit::Log::debug<LibFdb5>() << *this << " - Received [message=" << ((uint) message) << ",requestID=" << requestID << ",payloadSize=" << payload.size() << "]" << std::endl;
    // if (message == Message::Schema) {
    //     eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::handle received payload size: " << payload.size() << std::endl;
    //     MemoryStream s(payload);
    //     schema_ = std::unique_ptr<Schema>(eckit::Reanimator<Schema>::reanimate(s));
    //     return true;
    // }
    return false;
}

void RemoteCatalogue::handleException(std::exception_ptr e) {
    NOTIMP;
}

// Catalogue Reader
// DbStats RemoteCatalogue::stats() const {
//     NOTIMP;
// }
// bool RemoteCatalogue::axis(const std::string& keyword, eckit::StringSet& s) const {
//     NOTIMP;
// }
// bool RemoteCatalogue::retrieve(const InspectionKey& key, Field& field) const{
//     NOTIMP;
// }

void RemoteCatalogue::overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) {NOTIMP;}
void RemoteCatalogue::index(const InspectionKey& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) {NOTIMP;}
void RemoteCatalogue::reconsolidate(){NOTIMP;}
std::vector<eckit::PathName> RemoteCatalogue::metadataPaths() const {NOTIMP;}
void RemoteCatalogue::visitEntries(EntryVisitor& visitor, /*const Store& store,*/ bool sorted) {NOTIMP;}
void RemoteCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {NOTIMP;}
StatsReportVisitor* RemoteCatalogue::statsReportVisitor() const {NOTIMP;}
PurgeVisitor* RemoteCatalogue::purgeVisitor(const Store& store) const {NOTIMP;}
WipeVisitor* RemoteCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {NOTIMP;}
MoveVisitor* RemoteCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {NOTIMP;}
void RemoteCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {NOTIMP;}
std::vector<fdb5::Index> RemoteCatalogue::indexes(bool sorted) const {NOTIMP;}
void RemoteCatalogue::maskIndexEntry(const Index& index) const {NOTIMP;}
void RemoteCatalogue::allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const {NOTIMP;}
void RemoteCatalogue::print( std::ostream &out ) const {NOTIMP;}

std::string RemoteCatalogue::type() const {
    return "remote"; // RemoteEngine::typeName();
}
bool RemoteCatalogue::open() {
    return true;
}

static CatalogueReaderBuilder<RemoteCatalogue> reader("remote");
static CatalogueWriterBuilder<RemoteCatalogue> writer("remote");
} // namespace fdb5::remote