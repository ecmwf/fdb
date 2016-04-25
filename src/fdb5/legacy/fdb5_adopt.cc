/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/eckit.h"

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/config/Resource.h"
#include "eckit/runtime/Tool.h"
#include "eckit/runtime/Context.h"

#include "fdb5/legacy/FDBScanner.h"
#include "fdb5/legacy/IndexCache.h"

//----------------------------------------------------------------------------------------------------------------------

class FDBIndex : public eckit::Tool {

private:

    virtual void run();

public:

    FDBIndex(int argc, char **argv): eckit::Tool(argc,argv) {}

private:

    fdb5::legacy::IndexCache cache_;

};

void FDBIndex::run()
{
    eckit::Context& ctx = eckit::Context::instance();

    eckit::ThreadPool scanning;

    for(int i = 1; i < ctx.argc(); i++)
    {
        eckit::PathName path(ctx.argv(i));

        scanning.push( new fdb5::legacy::FDBScanner(cache_) );
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv)
{
    FDBIndex app(argc,argv);
    return app.start();
}

