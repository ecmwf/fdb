/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/remote/client/RemoteCatalogue.h"

#include "eckit/serialisation/ResizableMemoryStream.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Messages.h"

#include "eckit/filesystem/URI.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/rules/Schema.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

using namespace eckit;

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

namespace {

Schema* fetchSchema(const Key& dbKey, const RemoteCatalogue& catalogue) {
    LOG_DEBUG_LIB(LibFdb5) << "Fetching schema from remote catalogue: " << catalogue.controlEndpoint() << std::endl;

    // send dbkey to remote
    eckit::Buffer keyBuffer(RemoteCatalogue::defaultBufferSizeKey);
    eckit::MemoryStream keyStream(keyBuffer);
    keyStream << dbKey;

    const auto requestID = catalogue.generateRequestID();

    // receive schema from remote
    auto recvBuf = catalogue.controlWriteReadResponse(Message::Schema, requestID, keyBuffer, keyStream.position());

    eckit::MemoryStream schemaStream(recvBuf);
    return eckit::Reanimator<Schema>::reanimate(schemaStream);
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

RemoteCatalogue::RemoteCatalogue(const Key& key, const Config& config) :
    CatalogueImpl(key, {}, config), Client(config) {}

// Catalogue(URI, Config) is only used by the Visitors to traverse the catalogue. In the remote, we use the RemoteFDB
// for catalogue traversal this ctor is here only to comply with the factory
RemoteCatalogue::RemoteCatalogue(const eckit::URI& /*uri*/, const Config& config) : Client(config) {
    NOTIMP;
}

RemoteCatalogue::~RemoteCatalogue() = default;

void RemoteCatalogue::archive(const Key& idxKey, const Key& datumKey,
                              std::shared_ptr<const FieldLocation> fieldLocation) {

    ASSERT(!datumKey.empty());
    ASSERT(fieldLocation);

    uint32_t id = generateRequestID();
    {
        std::lock_guard<std::mutex> lock(archiveMutex_);
        if (numLocations_ == 0) {  // if this is the first archival request, notify the server
            controlWriteCheckResponse(Message::Archive, id, true);
        }
        numLocations_++;
    }

    Buffer buffer(defaultBufferSizeArchive);
    MemoryStream stream(buffer);
    stream << idxKey;
    stream << datumKey;
    stream << *fieldLocation;

    PayloadList payloads;
    payloads.emplace_back(stream.position(), buffer.data());

    dataWrite(Message::Blob, id, payloads);

    eckit::Log::status() << "FieldLocation " << numLocations_ << "enqueued for catalogue archival" << std::endl;
}

bool RemoteCatalogue::selectIndex(const Key& idxKey) {
    currentIndexKey_ = idxKey;
    return true;
}

bool RemoteCatalogue::createIndex(const Key& idxKey, size_t datumKeySize) {
    return true;
}

const Index& RemoteCatalogue::currentIndex() {
    NOTIMP;
}
const Key RemoteCatalogue::currentIndexKey() {
    return currentIndexKey_;
}

void RemoteCatalogue::deselectIndex() {
    currentIndexKey_ = Key();
}

const Schema& RemoteCatalogue::schema() const {
    // lazy loading schema
    if (!schema_) {
        schema_.reset(fetchSchema(dbKey_, *this));
        ASSERT(schema_);
    }
    return *schema_;
}

const Rule& RemoteCatalogue::rule() const {
    // lazy loading rule
    if (!rule_) {
        rule_ = std::cref(schema().matchingRule(dbKey_));
    }
    return rule_.value().get();
}

void RemoteCatalogue::flush(size_t archivedFields) {

    std::lock_guard<std::mutex> lock(archiveMutex_);
    ASSERT(archivedFields == numLocations_);

    // Flush only does anything if there is an ongoing archive();
    if (numLocations_ > 0) {

        eckit::Buffer sendBuf(defaultBufferSizeFlush);
        eckit::MemoryStream s(sendBuf);
        s << numLocations_;

        LOG_DEBUG_LIB(LibFdb5) << " RemoteCatalogue::flush - flushing " << numLocations_ << " fields" << std::endl;

        // The flush call is blocking
        controlWriteCheckResponse(Message::Flush, generateRequestID(), false, sendBuf, s.position());

        numLocations_ = 0;
    }
}

void RemoteCatalogue::clean() {
    NOTIMP;
}

void RemoteCatalogue::close() {
    NOTIMP;
}

bool RemoteCatalogue::exists() const {

    bool result = false;

    eckit::Buffer sendBuf(defaultBufferSizeKey);
    eckit::MemoryStream sms(sendBuf);
    sms << dbKey_;

    auto recvBuf = controlWriteReadResponse(Message::Exists, generateRequestID(), sendBuf, sms.position());

    eckit::MemoryStream rms(recvBuf);
    rms >> result;

    return result;
}

void RemoteCatalogue::checkUID() const {
    LOG_DEBUG_LIB(LibFdb5) << "RemoteCatalogue::checkUID() is noop!" << std::endl;
}

eckit::URI RemoteCatalogue::uri() const {
    return eckit::URI("fdb", controlEndpoint().host(), controlEndpoint().port());
}

void RemoteCatalogue::loadSchema() {
    // NB we're at the db level, so get the db schema. We will want to get the master schema beforehand.
    // (outside of the catalogue)
    schema();
}

const eckit::Configuration& RemoteCatalogue::clientConfig() const {
    return config();
}

bool RemoteCatalogue::handle(Message message, uint32_t requestID) {
    Log::warning() << *this << " - Received [message=" << ((uint)message) << ",requestID=" << requestID << "]"
                   << std::endl;
    return false;
}

bool RemoteCatalogue::handle(Message message, uint32_t requestID, eckit::Buffer&& payload) {
    LOG_DEBUG_LIB(LibFdb5) << *this << " - Received [message=" << ((uint)message) << ",requestID=" << requestID
                           << ",payloadSize=" << payload.size() << "]" << std::endl;
    return false;
}

void RemoteCatalogue::overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys,
                                bool unmount) {
    NOTIMP;
}
void RemoteCatalogue::index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) {
    NOTIMP;
}
void RemoteCatalogue::reconsolidate() {
    NOTIMP;
}
void RemoteCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    NOTIMP;
}
StatsReportVisitor* RemoteCatalogue::statsReportVisitor() const {
    NOTIMP;
}
PurgeVisitor* RemoteCatalogue::purgeVisitor(const Store& store) const {
    NOTIMP;
}
MoveVisitor* RemoteCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request,
                                          const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {
    NOTIMP;
}
void RemoteCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    NOTIMP;
}
std::vector<fdb5::Index> RemoteCatalogue::indexes(bool sorted) const {
    NOTIMP;
}

void RemoteCatalogue::allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                                std::set<eckit::URI>& data) const {
    NOTIMP;
}

bool RemoteCatalogue::uriBelongs(const eckit::URI& uri) const {
    // This functionality is deliberately not required for RemoteCatalogue, so assume false.
    return false;
}

void RemoteCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {
    // The server already knows which indexes to mask, just send the command
    eckit::Buffer sendBuf(256);
    eckit::ResizableMemoryStream s(sendBuf);
    s << dbKey_;
    controlWriteCheckResponse(Message::DoMaskIndexEntries, generateRequestID(), false, sendBuf, s.position());
}

CatalogueWipeState RemoteCatalogue::wipeInit() const {
    NOTIMP;
}

bool RemoteCatalogue::doWipeURIs(const CatalogueWipeState& wipeState) const {
    // Just tell the server to do it!
    eckit::Buffer sendBuf(256);
    eckit::ResizableMemoryStream s(sendBuf);
    s << dbKey_;

    controlWriteCheckResponse(Message::DoWipeURIs, generateRequestID(), false, sendBuf, s.position());
    return true;
}

bool RemoteCatalogue::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {
    // Send unknown uris to server
    eckit::Buffer sendBuf(unknownURIs.size() * 256);
    eckit::ResizableMemoryStream s(sendBuf);
    s << dbKey_;
    s << unknownURIs;

    controlWriteCheckResponse(Message::DoWipeUnknowns, generateRequestID(), false, sendBuf, s.position());

    return true;
}
void RemoteCatalogue::doWipeEmptyDatabase() const {
    // Tell server it can safely delete the DB if it is empty, and finish the wipe.
    eckit::Buffer sendBuf(256);
    eckit::ResizableMemoryStream s(sendBuf);
    s << dbKey_;

    controlWriteCheckResponse(Message::DoWipeFinish, generateRequestID(), false, sendBuf, s.position());
    return;
}

bool RemoteCatalogue::doUnsafeFullWipe() const {

    bool result = false;

    // send dbkey to remote
    eckit::Buffer keyBuffer(RemoteCatalogue::defaultBufferSizeKey);
    eckit::MemoryStream keyStream(keyBuffer);
    keyStream << dbKey_;

    // receive bool (full wipe supported or not) from remote
    auto recvBuf =
        controlWriteReadResponse(Message::DoUnsafeFullWipe, generateRequestID(), keyBuffer, keyStream.position());

    eckit::MemoryStream rms(recvBuf);
    rms >> result;

    return result;
}

void RemoteCatalogue::print(std::ostream& out) const {
    out << "RemoteCatalogue(endpoint=" << controlEndpoint() << ",clientID=" << clientId() << ")";
}

bool RemoteCatalogue::open() {
    return true;
}

//----------------------------------------------------------------------------------------------------------------------

static CatalogueReaderBuilder<RemoteCatalogue> reader(RemoteCatalogue::typeName());
static CatalogueWriterBuilder<RemoteCatalogue> writer(RemoteCatalogue::typeName());

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
