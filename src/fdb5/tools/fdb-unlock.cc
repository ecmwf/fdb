// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/tools/FDBLock.h"

using namespace eckit::option;
using namespace eckit;

int main(int argc, char** argv) {
    bool unlock = true;
    fdb5::tools::FDBLock app(argc, argv, unlock);
    return app.start();
}
