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
#include "eckit/io/FileHandle.h"

#include "marslib/MarsTask.h"

#include "fdb5/Retriever.h"
#include "fdb5/RequestParser.h"

using namespace std;
using namespace eckit;

class FDBExtract : public eckit::Tool {
    virtual void run();
public:
    FDBExtract(int argc,char **argv): Tool(argc,argv) {}
};

void FDBExtract::run()
{
    Context& ctx = Context::instance();

    ASSERT( ctx.argc() == 3 );

    ifstream in(ctx.argv(1));
    fdb5::RequestParser parser(in);

    MarsRequest r = parser.parse();
    std::cout << r << std::endl;


    MarsRequest e("environ");

    MarsTask task(r,e);

    fdb5::Retriever retriever(task);

    eckit::ScopedPtr<DataHandle> dh ( retriever.retrieve() );

    eckit::FileHandle out( ctx.argv(2));

    dh->saveInto(out);
}


int main(int argc, char **argv)
{
    FDBExtract app(argc,argv);
    return app.start();
}

