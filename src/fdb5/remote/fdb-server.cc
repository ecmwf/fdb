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

/// @author Simon Smart
/// @author Tiago Quintino
/// @date   Apr 2018

#include "fdb5/remote/FdbServer.h"

int main(int argc, char** argv) {
    fdb5::remote::FdbServer app(argc, argv, "FDB_HOME");
    app.start();
    return 0;
}
