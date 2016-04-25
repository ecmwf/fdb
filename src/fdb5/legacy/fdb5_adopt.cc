/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unistd.h>

#include "eckit/eckit.h"

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/config/Resource.h"
#include "eckit/runtime/Tool.h"
#include "eckit/runtime/Context.h"

#include "fdb5/legacy/FDBScanner.h"
#include "fdb5/legacy/IndexCache.h"

// Make sure this program has enough memory (ulimit)
// On AIX, the threads can corrupt the main stack and create unexpected behaviours
// Also, export AIXTHREAD_GUARDPAGES=1
// On AIX, set stack size to 1Mb

#ifdef _AIX
#define STACK_SIZE (1024*1024)
#else
#define STACK_SIZE (0)
#endif

//----------------------------------------------------------------------------------------------------------------------

class FDBIndex : public eckit::Tool {

private:

    virtual void run();

public:

    FDBIndex(int argc, char **argv) :
        eckit::Tool(argc,argv),
        scan_("scan",
              eckit::Resource<int>("-scans;numberOfScanThreads", 1),
              eckit::Resource<int>("-stack;scanThreadsStackSize", STACK_SIZE))
    {

    }

private:

    eckit::ThreadPool scan_;

};

void FDBIndex::run()
{
    eckit::Context& ctx = eckit::Context::instance();

    for(int i = 1; i < ctx.argc(); i++)
    {
        eckit::PathName path(ctx.argv(i));

        scan_.push( new fdb5::legacy::FDBScanner(path) );
    }

    for(;;)
    {
        ::sleep(120);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv)
{
    FDBIndex app(argc,argv);
    return app.start();
}

