/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/memory/NonCopyable.h"
#include "eckit/net/Endpoint.h"
#include "eckit/serialisation/MemoryStream.h"

#include <cstddef>  // std::size_t
#include <cstdint>  // std::uint32_t
#include <mutex>
#include <string>
#include <utility>  // std::pair
#include <vector>

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class RemoteFDBException : public eckit::RemoteException {
public:
    RemoteFDBException(const std::string& msg, const eckit::net::Endpoint& endpoint):
        eckit::RemoteException(msg, endpoint) {}
};

//----------------------------------------------------------------------------------------------------------------------

class Client : eckit::NonCopyable {
public:  // types
    using PayloadList  = Connection::PayloadList;
    using EndpointList = std::vector<std::pair<eckit::net::Endpoint, std::string>>;

    static constexpr size_t defaultBufferSizeArchive = 8192;
    static constexpr size_t defaultBufferSizeFlush   = 1024;
    static constexpr size_t defaultBufferSizeKey     = 4096;

public:  // methods
    Client(const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint);

    explicit Client(const EndpointList& endpoints);

    virtual ~Client();

    uint32_t clientId() const { return id_; }

    uint32_t id() const { return id_; }

    const eckit::net::Endpoint& controlEndpoint() const { return connection_.controlEndpoint(); }

    const std::string& defaultEndpoint() const { return connection_.defaultEndpoint(); }

    uint32_t generateRequestID() const { return connection_.generateRequestID(); }

    // blocking requests

    void controlWriteCheckResponse(Message msg, uint32_t requestID, bool dataListener, Payload payload = {}) const;

    void controlWriteCheckResponse(Message msg, uint32_t requestID, bool dataListener, const BufferStream& buffer) const {
        controlWriteCheckResponse(msg, requestID, dataListener, buffer.payload());
    }

    void controlWriteCheckResponse(Message     msg,
                                   uint32_t    requestID,
                                   bool        dataListener,
                                   const void* payload,
                                   uint32_t    payloadLength) const {
        controlWriteCheckResponse(msg, requestID, dataListener, {payloadLength, payload});
    }

    [[nodiscard]]
    eckit::Buffer controlWriteReadResponse(Message msg, uint32_t requestID, Payload payload = {}) const;

    [[nodiscard]]
    eckit::Buffer controlWriteReadResponse(Message msg, uint32_t requestID, const BufferStream& buffer) const {
        return controlWriteReadResponse(msg, requestID, buffer.payload());
    }

    [[nodiscard]]
    eckit::Buffer controlWriteReadResponse(Message     msg,
                                           uint32_t    requestID,
                                           const void* payload,
                                           uint32_t    payloadLength) const {
        return controlWriteReadResponse(msg, requestID, {payloadLength, payload});
    }
    void dataWrite(Message msg, uint32_t requestID, PayloadList payloads = {});

    // handlers for incoming messages - to be defined in the client class

    virtual bool handle(Message message, uint32_t requestID) = 0;

    virtual bool handle(Message message, uint32_t requestID, eckit::Buffer&& payload) = 0;

protected:
    ClientConnection& connection_;

private:
    void setClientID();

private:
    uint32_t id_;

    mutable std::mutex blockingRequestMutex_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
