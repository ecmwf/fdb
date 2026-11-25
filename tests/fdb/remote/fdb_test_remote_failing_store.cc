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

    fdb5::remote::FdbServer app(argc, argv, "FDB_HOME");

    // n.b. registered once the application is initialised, i.e. once the plugins are loaded: LibFdb5 holds a single
    // constructor callback, which a plugin may register when it is loaded (e.g. that of GribJump), replacing one
    // registered earlier. That callback is kept, and called first.
    fdb5::ConstructorCallback previous = fdb5::LibFdb5::instance().constructorCallback();
    fdb5::LibFdb5::instance().registerConstructorCallback([previous](fdb5::CallbackRegistry& registry) {
        previous(registry);
        registry.registerFlushCallback([] {
            // n.b. while a request is handled, its trace context (and trace state) is the current one
            if (fdb5::tracing::current().traceState().find("fdbtest=failflush") != std::string::npos) {
                throw eckit::SeriousBug("Flush failure injected by the test (fdbtest=failflush)", Here());
            }
        });
    });

    app.start();
    return 0;
}
