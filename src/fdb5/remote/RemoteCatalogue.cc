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

#include "fdb5/LibFdb5.h"
#include "fdb5/remote/RemoteCatalogue.h"
#include "eckit/serialisation/MemoryStream.h"

#include <chrono> // xxx debug / development
#include <thread> // xxx debug / development


using namespace eckit;
namespace fdb5::remote {

RemoteCatalogue::RemoteCatalogue(const Key& key, const Config& config):
    CatalogueImpl(key, ControlIdentifiers(), config), // xxx what are control identifiers? Setting empty here...
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port"))), // xxx hardcoded endpoint
    config_(config), schema_(nullptr),
    maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200))
    {
        loadSchema();
    }

RemoteCatalogue::RemoteCatalogue(const eckit::URI& uri, const Config& config):
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port"))),  schema_(nullptr),
    maxArchiveQueueLength_(eckit::Resource<size_t>("fdbRemoteArchiveQueueLength;$FDB_REMOTE_ARCHIVE_QUEUE_LENGTH", 200))
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
    keyStream << dbKey_;
    keyStream << currentIndexKey_;
    keyStream << key;

    Buffer locBuffer(4096);
    MemoryStream locStream(locBuffer);
    locStream << *fieldLocation;

    MessageHeader message(Message::Blob, 0, id, keyStream.position() + locStream.position());
    dataWrite(id, &message, sizeof(message));
    dataWrite(id, keyBuffer, keyStream.position());
    dataWrite(id, locBuffer, locStream.position());
    dataWrite(id, &EndMarker, sizeof(EndMarker));
}

FDBStats RemoteCatalogue::archiveThreadLoop(){
    FDBStats localStats;
    eckit::Timer timer;

    std::pair<uint32_t, std::pair<Key, std::unique_ptr<FieldLocation> > >  element;

    try {

        ASSERT(archiveQueue_);
        long popped;
        while ((popped = archiveQueue_->pop(element)) != -1) {
            timer.start();
            eckit::Log::debug<LibFdb5>() << " RemoteCatalogue::archiveThreadLoop() popped: " << popped << " id: " << element.first << 
                " key: " << element.second.first << " fieldLocation: " << element.second.second->uri() << std::endl;
            sendArchiveData(element.first, element.second.first, std::move(element.second.second));
            timer.stop();

            localStats.addArchive(0, timer, 1);

        }

        dataWrite(Message::Flush, nullptr, 0);
        archiveQueue_.reset();

    } catch (...) {
        archiveQueue_->interrupt(std::current_exception());
        throw;
    }

    return localStats;
}

void RemoteCatalogue::archive(const InspectionKey& key, std::unique_ptr<FieldLocation> fieldLocation) {
    
    // if there is no archiving thread active, then start one.
    // n.b. reset the archiveQueue_ after a potential flush() cycle.
    if (!archiveFuture_.valid()) {

        // Start the archival request on the remote side

        // Reset the queue after previous done/errors
        {
            std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
            ASSERT(!archiveQueue_);
            archiveQueue_.reset(new ArchiveQueue(maxArchiveQueueLength_));
        }

        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }

    ASSERT(archiveFuture_.valid());

    // xxx I don't think we need to to this controlwrite everytime? On the remote side, the first call starts the archive thread loop.
    // xxx repeated calls dont do anything? Its already listening for archive requests on the data connection after the first call.
    // xxx so, try only doing this call on the first archive request.
    uint32_t id = controlWriteCheckResponse(Message::Archive); 

    std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
    ASSERT(archiveQueue_);

    eckit::Log::debug<LibFdb5>() << " RemoteCatalogue::archive() added to queue with id: " << id << 
        " key: " << key << " fieldLocation: " << fieldLocation->uri() << std::endl;

    archiveQueue_->emplace(std::make_pair(id, std::make_pair(key, std::move(fieldLocation))));

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
    if (archiveFuture_.valid()) {

        ASSERT(archiveQueue_);
        std::lock_guard<std::mutex> lock(archiveQueuePtrMutex_);
        archiveQueue_->close();

        FDBStats stats = archiveFuture_.get();
        ASSERT(!archiveQueue_);

        ASSERT(stats.numFlush() == 0);
        size_t numArchive = stats.numArchive();

        Buffer sendBuf(4096);
        MemoryStream s(sendBuf);
        s << numArchive;

        // The flush call is blocking
        controlWriteCheckResponse(fdb5::remote::Message::Flush, sendBuf, s.position());

//        internalStats_ += stats;
    }

    timer.stop();
//    internalStats_.addFlush(timer);
}

void RemoteCatalogue::clean() {NOTIMP;}

void RemoteCatalogue::close() {NOTIMP;}

bool RemoteCatalogue::exists() const {NOTIMP;}

void RemoteCatalogue::checkUID() const {NOTIMP;}

// xxx what is this uri used for?
eckit::URI RemoteCatalogue::uri() const {
    // return eckit::URI(TocEngine::typeName(), basePath());
    return eckit::URI("remotecat", ""); // todo
}

void RemoteCatalogue::loadSchema() {
    // NB we're at the db level, so get the db schema. We will want to get the master schema beforehand.
    // (outside of the catalogue) 

    eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::loadSchema()" << std::endl;

    // destroy previous schema if it exists
    schema_.reset(nullptr);

    // send dbkey to remote.
    constexpr uint32_t bufferSize = 4096;
    eckit::Buffer keyBuffer(bufferSize);
    eckit::MemoryStream keyStream(keyBuffer);
    keyStream << dbKey_;
    
    uint32_t id = controlWriteCheckResponse(Message::Schema, keyBuffer, keyStream.position());
    eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::loadSchema() received id: " << id << std::endl;

    // Should block - Use control connection.
    // XXX This is a temporary work around while control read is being reimplemented.
    while (!schema_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

}

bool RemoteCatalogue::handle(Message message, uint32_t requestID) {
    eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::handle received message: " << ((uint) message) << " - requestID: " << requestID << std::endl;
    NOTIMP;
    return false;
}
bool RemoteCatalogue::handle(Message message, uint32_t requestID, eckit::net::Endpoint endpoint, eckit::Buffer&& payload) {
    eckit::Log::debug<LibFdb5>()<< "RemoteCatalogue::handle received message: " << ((uint) message) << " - requestID: " << requestID << std::endl;
    if (message == Message::Schema) {
        eckit::Log::debug<LibFdb5>() << "RemoteCatalogue::handle received payload size: " << payload.size() << std::endl;
        MemoryStream s(payload);
        schema_ = std::unique_ptr<Schema>(eckit::Reanimator<Schema>::reanimate(s));
        return true;
    }
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
    NOTIMP;
}
bool RemoteCatalogue::open() {
    NOTIMP;
}

static CatalogueWriterBuilder<RemoteCatalogue> builder("remote");
} // namespace fdb5::remote