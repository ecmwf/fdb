/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_remote_RemoteFDB_H
#define fdb5_remote_RemoteFDB_H

#include <future>
#include <thread>

#include "fdb5/api/FDB.h"
#include "fdb5/api/FDBFactory.h"

#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPStream.h"
#include "eckit/io/Buffer.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/container/Queue.h"


namespace fdb5 {

class FDB;

namespace remote {
    class MessageHeader;
}

//----------------------------------------------------------------------------------------------------------------------


class RemoteFDB : public FDBBase {

public: // method

    RemoteFDB(const eckit::Configuration& config);
    ~RemoteFDB();

    /// Archive writes data into aggregation buffer
    virtual void archive(const Key& key, const void* data, size_t length);

    virtual eckit::DataHandle* retrieve(const MarsRequest& request);

    virtual std::string id() const;

    virtual void flush();

    virtual FDBStats stats() const;

private: // methods

    void connect();
    void disconnect();

    void clientWrite(const void* data, size_t length);
    void clientRead(void* data, size_t length);
    void handleError(const remote::MessageHeader& hdr);

    /// Do the actual communication with the server

    void doBlockingFlush();
    void doBlockingArchive(const Key& key, const eckit::Buffer& data);
    FDBStats archiveThreadLoop();
//    void sendArchiveBuffer();

    virtual void print(std::ostream& s) const;

private: // members

    std::string hostname_;
    int port_;

    eckit::TCPClient client_;

#if 0
    eckit::ScopedPtr<eckit::Buffer> archiveBuffer_;
    size_t archivePosition_;
#endif

    eckit::Queue<std::pair<fdb5::Key, eckit::Buffer>> archiveQueue_;

    std::future<FDBStats> archiveFuture_;

    FDBStats internalStats_;

    bool connected_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_remote_RemoteFDB_H
