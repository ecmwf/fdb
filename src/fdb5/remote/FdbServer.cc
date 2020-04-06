/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "eckit/thread/ThreadControler.h"

#include "fdb5/remote/FdbServer.h"

#include "fdb5/remote/AvailablePortList.h"
#include "fdb5/remote/Handler.h"

using namespace eckit;


namespace fdb5 {
namespace remote {

//----------------------------------------------------------------------------------------------------------------------

FDBForker::FDBForker(net::TCPSocket &socket, const Config &config) :
    ProcessControler(true),
    socket_(socket),
    config_(config) {}

FDBForker::~FDBForker() {}

void FDBForker::run() {

    eckit::Monitor::instance().reset(); // needed to the monitor to work on forked (but not execed process)

    // Ensure random state is reset after fork
    ::srand(::getpid() + ::time(nullptr));
    ::srandom(::getpid() + ::time(nullptr));

    eckit::Log::info() << "FDB forked pid " << ::getpid() << std::endl;

    RemoteHandler handler(socket_, config_);
    handler.handle();
}

//----------------------------------------------------------------------------------------------------------------------

FDBServerThread::FDBServerThread(net::TCPSocket& socket, const Config& config) :
    socket_(socket),
    config_(config) {}

void FDBServerThread::run() {
    eckit::Log::info() << "FDB started handler thread" << std::endl;

    RemoteHandler handler(socket_, config_);
    handler.handle();
}

//----------------------------------------------------------------------------------------------------------------------

FdbServerBase::FdbServerBase() {}

FdbServerBase::~FdbServerBase() {}

void FdbServerBase::doRun() {

    hookUnique();

    Config config = LibFdb5::instance().defaultConfig();
    config.set("statistics", true);

    // If we are using a specified range of ports, start a thread that
    // maintains the list of available ports.
    startPortReaperThread(config);

    int port = config.getInt("serverPort", 7654);
    bool threaded = config.getBool("serverThreaded", false);

    net::TCPServer server(net::Port("fdb", port), net::SocketOptions::server().reusePort(true));
    server.closeExec(false);

    port_ = server.localPort();

    while (true) {
        try {
            if (threaded) {
                ThreadControler t(new FDBServerThread(server.accept(), config));
                t.start();
            }
            else {
                FDBForker f(server.accept(), config);
                f.start();
            }
        }
        catch (std::exception& e) {
            eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
            eckit::Log::error() << "** Exception is ignored" << std::endl;
        }
    }
}

void FdbServerBase::startPortReaperThread(const Config &config) {

    if (config.has("dataPortStart")) {
        ASSERT(config.has("dataPortCount"));

        int startPort = config.getInt("dataPortStart");
        size_t count = config.getLong("dataPortCount");

        eckit::Log::info() << "Using custom port list. startPort=" << startPort
                           << ", count=" << count << std::endl;

        AvailablePortList portList(startPort, count);
        portList.initialise();

        reaperThread_ = std::thread([startPort, count]() {

            AvailablePortList portList(startPort, count);

            while (true) {
                portList.reap(120);
                std::this_thread::sleep_for(std::chrono::seconds(10));
            }
        });
    }

}

//----------------------------------------------------------------------------------------------------------------------

FdbServer::FdbServer(int argc, char **argv, const char *home) :
    eckit::Application(argc, argv, home),
    FdbServerBase()
{
}

FdbServer::~FdbServer() {}

void FdbServer::run() {
    doRun();
}

void FdbServer::hookUnique() {}

//----------------------------------------------------------------------------------------------------------------------

} // namespace remote
} // namespace fdb5
