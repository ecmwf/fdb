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
#include "eckit/net/TCPStream.h"
#include "eckit/net/Port.h"

#include <unistd.h>


//----------------------------------------------------------------------------------------------------------------------

class FdbServer : public eckit::Application,
                  public eckit::Monitorable {

public: // methods

    FdbServer(int argc, char** argv) :
        Application(argc, argv, "FDB_HOME") {}

private: // methods

    // From Application

    virtual void run();

    // From monitorable

    virtual void status(std::ostream&) const;
    virtual void json(eckit::JSON&) const;

private: // members

    size_t port_;
};


//----------------------------------------------------------------------------------------------------------------------

using namespace eckit;

void FdbServer::run() {

    Log::status() << "Starting server ..." << port_ << std::endl;

    TCPServer server(Port("fdb", 7654));

    while (true) {

        TCPStream in(server.accept());
        Log::info() << "Received connection" << std::endl;
        Log::status() << "Received connection" << std::endl;

        while (true) {
            uint32_t tmp;
            in >> tmp;
            Log::info() << "Read: " << tmp << std::endl;
            if (tmp == 12) break;
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

int main(int argc, char** argv) {
    FdbServer app(argc, argv);
    app.start();
    return 0;
}
