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

#include "eckit/exception/Exceptions.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/os/BackTrace.h"

#include "fdb5/remote/Messages.h"

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

public: // methods
    Connection();
    virtual ~Connection();

    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, const void* data, uint32_t length);
    void write(Message msg, bool control, uint32_t clientID, uint32_t requestID, std::vector<std::pair<const void*, uint32_t>> data = {});

    void error(const std::string& msg, uint32_t clientID, uint32_t requestID);

    eckit::Buffer readControl(MessageHeader& hdr);
    eckit::Buffer readData(MessageHeader& hdr);

    void teardown();

private: // methods

    eckit::Buffer read(bool control, MessageHeader& hdr);

    void writeUnsafe(bool control, const void* data, size_t length);
    void readUnsafe(bool control, void* data, size_t length);

    virtual eckit::net::TCPSocket& controlSocket() = 0;
    virtual eckit::net::TCPSocket& dataSocket() = 0;

protected: // members

    bool single_;

private: // members

    std::mutex controlMutex_;
    std::mutex dataMutex_;
    std::mutex readControlMutex_;
    std::mutex readDataMutex_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
