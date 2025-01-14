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

#include "fdb5/remote/Messages.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/os/BackTrace.h"

#include <cstdint>
#include <iostream>
#include <mutex>
#include <utility>
#include <vector>

namespace eckit {

class Buffer;

}

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class TCPException : public eckit::Exception {
public:
    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {

        std::cerr << "TCP Exception; backtrace(): " << std::endl;
        std::cerr << eckit::BackTrace::dump() << std::endl;
    }
};

//----------------------------------------------------------------------------------------------------------------------

class Connection : eckit::NonCopyable {

public:  // types
    using Payload = std::vector<std::pair<const void*, uint32_t>>;

public: // methods
    Connection();

    virtual ~Connection() = default;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, Payload data = {}) const;

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, const void* data, uint32_t length) const;

    void error(const std::string& msg, uint32_t clientID, uint32_t requestID) const;

    eckit::Buffer readControl(MessageHeader& hdr) const;

    eckit::Buffer readData(MessageHeader& hdr) const;

    void teardown();

private: // methods
    eckit::Buffer read(bool control, MessageHeader& hdr) const;

    void writeUnsafe(bool control, const void* data, size_t length) const;

    void readUnsafe(bool control, void* data, size_t length) const;

    virtual const eckit::net::TCPSocket& controlSocket() const = 0;

    virtual const eckit::net::TCPSocket& dataSocket() const = 0;

protected: // members

    bool single_;

private: // members
    mutable std::mutex controlMutex_;
    mutable std::mutex dataMutex_;
    mutable std::mutex readControlMutex_;
    mutable std::mutex readDataMutex_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
