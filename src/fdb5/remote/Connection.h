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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string_view>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/os/BackTrace.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/remote/Messages.h"

namespace eckit {

class Buffer;

}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public eckit::Exception {
public:

    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {}
};

//----------------------------------------------------------------------------------------------------------------------

class Connection : eckit::NonCopyable {

public:  // types

    using PayloadList = std::vector<Payload>;

public:  // methods

    Connection();

    virtual ~Connection() = default;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, PayloadList payloads = {}) const;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, const void* data,
               uint32_t length) const {
        write(msg, control, clientID, requestID, {{length, data}});
    }

    void error(std::string_view msg, uint32_t clientID, uint32_t requestID) const;

    eckit::Buffer readControl(MessageHeader& hdr) const;

    eckit::Buffer readData(MessageHeader& hdr) const;

    void teardown();

    bool valid() const { return isValid_; }

private:  // methods

    eckit::Buffer read(bool control, MessageHeader& hdr) const;

    void writeUnsafe(bool control, const void* data, size_t length) const;

    bool readUnsafe(bool control, void* data, size_t length) const;

    virtual const eckit::net::TCPSocket& controlSocket() const = 0;

    virtual const eckit::net::TCPSocket& dataSocket() const = 0;

protected:  // members

    bool single_;

private:  // members

    bool closingSocket_ = false;

    mutable std::mutex controlMutex_;
    mutable std::mutex dataMutex_;

    /// Indicates if this instance is in a usable state.
    /// Once this is marked as invalid it cannot be recovered.
    mutable std::atomic<bool> isValid_{true};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
