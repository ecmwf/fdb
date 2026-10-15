// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
    Config config_;
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

    FdbServerBase(const FdbServerBase&) = delete;
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
