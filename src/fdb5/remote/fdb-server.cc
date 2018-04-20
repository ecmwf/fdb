/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Monitorable.h"
#include "eckit/runtime/Application.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/net/Port.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/thread/ThreadControler.h"

#include "fdb5/remote/Handler.h"
#include "fdb5/config/Config.h"

#include <unistd.h>


using namespace eckit;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

class FdbServer : public Application,
                  public Monitorable {

public: // methods

    FdbServer(int argc, char** argv) :
        Application(argc, argv, "FDB_HOME"),
        subProcesses_(false) {}

private: // methods

    // From Application

    virtual void run();

    // From monitorable

    virtual void status(std::ostream&) const;
    virtual void json(JSON&) const;

private: // members

    bool subProcesses_;

    size_t port_;
};


//----------------------------------------------------------------------------------------------------------------------


class FdbServerThread : public Thread {
    TCPSocket socket_;
    bool subProcess_;

    // TODO: Use non-default configs

    virtual void run() {
        if (subProcess_) {
            Log::info() << "Starting handler process ..." << std::endl;
            RemoteHandlerProcessController h(socket_, fdb5::Config());
            h.start();
        } else {
            RemoteHandler h(socket_, fdb5::Config());
            h.handle();
        }
    }

public:
    FdbServerThread(eckit::TCPSocket& socket, bool launchSubProcess) :
        socket_(socket),
        subProcess_(launchSubProcess) {}
};


//----------------------------------------------------------------------------------------------------------------------


void FdbServer::run() {

    Log::status() << "Starting server ..." << port_ << std::endl;

    TCPServer server(Port("fdb", 7654));
    server.closeExec(false);

    while (true) {

//        TCPStream in(server.accept());
        TCPSocket in(server.accept());

        Log::info() << "Received connection" << std::endl;
        Log::status() << "Received connection" << std::endl;

        // Fork a process, or spawn a thread, to deal with this connection

        try {
            Log::info() << "Starting handler thread ..." << std::endl;
            ThreadControler t(new FdbServerThread(in, subProcesses_));
            t.start();
        } catch (std::exception& e) {
            Log::error() << "** " << e.what() << " Caught in FdbServer::run()" << std::endl;
            Log::error() << "** Exception is ignored" << std::endl;
        }
    }

    Log::status() << "done" << std::endl;
}

void FdbServer::status(std::ostream& s) const {
    s << "STATUS";

}

void FdbServer::json(JSON& j) const {

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5


int main(int argc, char** argv) {
    fdb5::remote::FdbServer app(argc, argv);
    app.start();
    return 0;
}
