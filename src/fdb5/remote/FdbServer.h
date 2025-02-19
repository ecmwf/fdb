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
/// @author Tiagop Quintino
/// @date   Nov 2019

#pragma once

#include <unistd.h>
#include <thread>

#include "eckit/net/Port.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/Application.h"
#include "eckit/runtime/Monitor.h"
#include "eckit/runtime/Monitorable.h"
#include "eckit/runtime/ProcessControler.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/config/Config.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------


class FDBForker : public eckit::ProcessControler {

public:  // methods

    FDBForker(eckit::net::TCPSocket& socket, const Config& config);
    ~FDBForker() override;

private:  // methods

    void run() override;

    eckit::net::TCPSocket socket_;
    eckit::LocalConfiguration config_;
};

//----------------------------------------------------------------------------------------------------------------------

class FdbServerBase {
public:

    FdbServerBase();

    virtual ~FdbServerBase();

    virtual void doRun();

private:

    int port_;
    std::thread reaperThread_;

    FdbServerBase(const FdbServerBase&)            = delete;
    FdbServerBase& operator=(const FdbServerBase&) = delete;

    virtual void hookUnique() = 0;

    void startPortReaperThread(const Config& config);
};

//----------------------------------------------------------------------------------------------------------------------

class FdbServer : public eckit::Application, public FdbServerBase {
public:

    FdbServer(int argc, char** argv, const char* home);

    ~FdbServer() override;

    void run() override;

    void hookUnique() override;  // non-unique
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote
