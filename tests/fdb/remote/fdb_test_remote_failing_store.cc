// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// An fdb-server for tests, whose store flush fails on request: when the trace state of the request contains
// "fdbtest=failflush". A deterministic failure, that neither timing nor privileges affect.
//
// The server is the regular FdbServer: the failure is injected through the flush callback of the StoreHandler,
// registered by the constructor callback of LibFdb5 (called by each StoreHandler).

#include <string>

#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/Callback.h"
#include "fdb5/remote/FdbServer.h"
#include "fdb5/tracing/TraceContext.h"

int main(int argc, char** argv) {

    fdb5::LibFdb5::instance().registerConstructorCallback([](fdb5::CallbackRegistry& registry) {
        registry.registerFlushCallback([] {
            // n.b. while a request is handled, its trace context (and trace state) is the current one
            if (fdb5::tracing::current().traceState().find("fdbtest=failflush") != std::string::npos) {
                throw eckit::SeriousBug("Flush failure injected by the test (fdbtest=failflush)", Here());
            }
        });
    });

    fdb5::remote::FdbServer app(argc, argv, "FDB_HOME");
    app.start();
    return 0;
}
