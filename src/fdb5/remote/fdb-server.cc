// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
