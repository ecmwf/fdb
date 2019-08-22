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

#include "eckit/cmd/CmdApplication.h"
#include "eckit/runtime/Application.h"


//----------------------------------------------------------------------------------------------------------------------

class FdbMonitor : public eckit::Application, public eckit::CmdApplication {

public:
    FdbMonitor(int argc, char** argv) :
        Application(argc, argv, "FDB_HOME") {}

    virtual void run() {
        eckit::CmdApplication::execute();
    }
};


//----------------------------------------------------------------------------------------------------------------------

int main(int argc,char **argv) {
    FdbMonitor app(argc,argv);
    app.start();
    return 0;
}
