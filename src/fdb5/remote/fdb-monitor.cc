// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "eckit/cmd/CmdApplication.h"
#include "eckit/runtime/Application.h"


//----------------------------------------------------------------------------------------------------------------------

class FdbMonitor : public eckit::Application, public eckit::CmdApplication {

public:

    FdbMonitor(int argc, char** argv) : Application(argc, argv, "FDB_HOME") {}

    virtual void run() { eckit::CmdApplication::execute(); }
};


//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    FdbMonitor app(argc, argv);
    app.start();
    return 0;
}
