/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Tool.h"
#include "eckit/runtime/Context.h"

#include "fdb5/grib/GribIndexer.h"

using namespace eckit;

class FDBIndex : public eckit::Tool {
    virtual void run();
public:
    FDBIndex(int argc, char **argv): Tool(argc, argv) {}
};

void FDBIndex::run() {
    Context &ctx = Context::instance();

    fdb5::GribIndexer indexer;

    for (int i = 1; i < ctx.argc(); i++) {
        eckit::PathName path(ctx.argv(i));

        std::cout << "Processing " << path << std::endl;

        indexer.index(path);
    }
}


int main(int argc, char **argv) {
    FDBIndex app(argc, argv);
    return app.start();
}

