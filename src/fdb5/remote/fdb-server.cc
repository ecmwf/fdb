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
/// @author Tiago Quintino
/// @date   Apr 2018

#include <unistd.h>

#include "eckit/runtime/Monitorable.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/net/Port.h"
#include "eckit/runtime/ProcessControler.h"
#include "eckit/runtime/Monitor.h"
#include "eckit/thread/ThreadControler.h"

#include "fdb5/remote/Handler.h"
#include "fdb5/config/Config.h"

#include "common/MarsApplication.h"

using namespace eckit;

namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

class FdbServerThread : public Thread {
    TCPSocket socket_;
    bool subProcess_;

    // TODO: Use non-default configs

    virtual void run() {
        if (subProcess_) {
            Log::info() << "Starting handler process ..." << std::endl;
            RemoteHandlerProcessController h(socket_);
            h.start();
        } else {
            RemoteHandler h(socket_);
            h.handle();
        }
    }

public:
    FdbServerThread(eckit::TCPSocket& socket, bool launchSubProcess) :
        socket_(socket),
        subProcess_(launchSubProcess) {
    }
};

//----------------------------------------------------------------------------------------------------------------------

class FdbServer : public MarsApplication,
                  public Monitorable {

public: // methods

    FdbServer(int argc, char** argv) :
        MarsApplication(argc, argv, "FDB_HOME"),
        forkSubProcess_(eckit::Resource<bool>("-fork;forkSubProcess", 0)) {}

private: // methods

    // From Application

    virtual void run();

    // From monitorable

    virtual void status(std::ostream&) const;
    virtual void json(JSON&) const;

private: // members

    bool forkSubProcess_;

    size_t port_;
};


void FdbServer::run() {

    Log::status() << "Starting server ..." << port_ << std::endl;

    TCPServer server(Port("fdb", 7654));
    server.closeExec(false);

    while (true) {

        TCPSocket in(server.accept());

        Log::info() << "Received connection" << std::endl;
        Log::status() << "Received connection" << std::endl;

        // Fork a process, or spawn a thread, to deal with this connection

        try {
            Log::info() << "Starting handler thread ..." << std::endl;
            ThreadControler t(new FdbServerThread(in, forkSubProcess_));
            t.start();
        } catch (std::exception& e) {
            Log::error() << "** " << e.what() << " Caught in FdbServer::run()" << std::endl;
            Log::error() << "** Exception is ignored" << std::endl;
        }
    }
}

void FdbServer::status(std::ostream& s) const {
    s << "Running";
}

void FdbServer::json(JSON&) const {

}

//--------------------------------------------------------------------------------------------------

class FDBForker : public RemoteHandler
                , public eckit::ProcessControler {

public: // methods

    FDBForker(eckit::TCPSocket& socket, const Config& config = fdb5::Config()) : RemoteHandler(socket) {
    }

private: // methods

    virtual void run() {

        Log::info() << "FDB forked pid " << ::getpid() << std::endl;

        Monitor::instance().reset(); // needed to the monitor to work on forked (but not execed process)

        handle();
    }

};

//--------------------------------------------------------------------------------------------------

class FDBSvrApp : public MarsApplication {
public:
    FDBSvrApp(int argc, char** argv) :
        MarsApplication(argc, argv, "FDB_HOME") {
    }

    ~FDBSvrApp() {}

private:

    int port_;

    FDBSvrApp(const FDBSvrApp&) = delete;
    FDBSvrApp& operator=(const FDBSvrApp&) = delete;

    virtual void run() {
        unique();

        TCPServer server(Port("fdb", 7654));
        server.closeExec(false);

        port_ = server.localPort();

        while (true) {
            try {
                FDBForker f(server.accept());
                f.start();
            }
            catch (std::exception& e) {
                Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
                Log::error() << "** Exception is ignored" << std::endl;
            }
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5


int main(int argc, char** argv) {

#if 0
    fdb5::remote::FdbServer app(argc, argv);
#else
    fdb5::remote::FDBSvrApp app(argc, argv);
#endif
    app.start();
    return 0;
}
