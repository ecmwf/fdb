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
#include "fdb5/remote/client/RemoteCatalogue.h"

#include <unordered_map>

using namespace eckit;
namespace fdb5::remote {

RemoteCatalogue::RemoteCatalogue(const Key& key, const Config& config):
    CatalogueImpl(key, ControlIdentifiers(), config), // xxx what are control identifiers? Setting empty here...
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port")), ""),
    config_(config), schema_(nullptr), numLocations_(0) {

    loadSchema();
}

RemoteCatalogue::RemoteCatalogue(const eckit::URI& uri, const Config& config):
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port")), ""), config_(config), schema_(nullptr), numLocations_(0)
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

void RemoteCatalogue::archive(const Key& idxKey, const InspectionKey& key, std::unique_ptr<FieldLocation> fieldLocation) {

    ASSERT(!key.empty());
    ASSERT(fieldLocation);

    uint32_t id = connection_.generateRequestID();
    {
        std::lock_guard<std::mutex> lock(archiveMutex_);
        if (numLocations_ == 0) { // if this is the first archival request, notify the server
            controlWriteCheckResponse(Message::Archive, id, true);
        }
        numLocations_++;
    }

    Buffer buffer(8192);
    MemoryStream stream(buffer);
    stream << idxKey;
    stream << key;
    stream << *fieldLocation;

    std::vector<std::pair<const void*, uint32_t>> payloads;
    payloads.push_back(std::pair<const void*, uint32_t>{buffer, stream.position()});

    dataWrite(Message::Blob, id, payloads);

    eckit::Log::status() << "FieldLocation " << numLocations_ << "enqueued for catalogue archival" << std::endl;
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

void RemoteCatalogue::flush(size_t archivedFields) {

    std::lock_guard<std::mutex> lock(archiveMutex_);

    ASSERT(archivedFields == numLocations_);

    // Flush only does anything if there is an ongoing archive();
    if (numLocations_ > 0) {

        Buffer sendBuf(1024);
        MemoryStream s(sendBuf);
        s << numLocations_;

        LOG_DEBUG_LIB(LibFdb5) << " RemoteCatalogue::flush - flushing " << numLocations_ << " fields" << std::endl;

        // The flush call is blocking
        uint32_t id = generateRequestID();
        controlWriteCheckResponse(Message::Flush, id, false, sendBuf, s.position());

        numLocations_ = 0;
    }
}

void RemoteCatalogue::clean() {NOTIMP;}

void RemoteCatalogue::close() {NOTIMP;}

bool RemoteCatalogue::exists() const {NOTIMP;}

void RemoteCatalogue::checkUID() const {}

eckit::URI RemoteCatalogue::uri() const {
    return eckit::URI("fdb", controlEndpoint().host(), controlEndpoint().port());
}

void RemoteCatalogue::loadSchema() {
    // NB we're at the db level, so get the db schema. We will want to get the master schema beforehand.
    // (outside of the catalogue) 

    if (!schema_) {
        LOG_DEBUG_LIB(LibFdb5) << "RemoteCatalogue::loadSchema()" << std::endl;

        // send dbkey to remote.
        eckit::Buffer keyBuffer(4096);
        eckit::MemoryStream keyStream(keyBuffer);
        keyStream << dbKey_;
        
        uint32_t id = generateRequestID();
        eckit::Buffer buf = controlWriteReadResponse(Message::Schema, id, keyBuffer, keyStream.position()).get();

        eckit::MemoryStream s(buf);
        schema_.reset(eckit::Reanimator<fdb5::Schema>::reanimate(s));
    }
}

bool RemoteCatalogue::handle(Message message, bool control, uint32_t requestID) {
    LOG_DEBUG_LIB(LibFdb5) << *this << " - Received [message=" << ((uint) message) << ",requestID=" << requestID << "]" << std::endl;
    NOTIMP;
    return false;
}
bool RemoteCatalogue::handle(Message message, bool control, uint32_t requestID, eckit::Buffer&& payload) {
    LOG_DEBUG_LIB(LibFdb5) << *this << " - Received [message=" << ((uint) message) << ",requestID=" << requestID << ",payloadSize=" << payload.size() << "]" << std::endl;
    // if (message == Message::Schema) {
    //     LOG_DEBUG_LIB(LibFdb5) << "RemoteCatalogue::handle received payload size: " << payload.size() << std::endl;
    //     MemoryStream s(payload);
    //     schema_ = std::unique_ptr<Schema>(eckit::Reanimator<Schema>::reanimate(s));
    //     return true;
    // }
    return false;
}

void RemoteCatalogue::handleException(std::exception_ptr e) {
    NOTIMP;
}

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
void RemoteCatalogue::print( std::ostream &out ) const {
    out << "RemoteCatalogue(endpoint=" << controlEndpoint() << ",clientID=" << clientId() << ")";
}


std::string RemoteCatalogue::type() const {
    return "remote";
}
bool RemoteCatalogue::open() {
    return true;
}

static CatalogueReaderBuilder<RemoteCatalogue> reader("remote");
static CatalogueWriterBuilder<RemoteCatalogue> writer("remote");
} // namespace fdb5::remote