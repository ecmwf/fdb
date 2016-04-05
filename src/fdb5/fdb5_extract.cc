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

using namespace eckit;

class FdbExtract : public eckit::Tool {
    virtual void run();
public:
    FdbExtract(int argc,char **argv): Tool(argc,argv) {}
};

void FdbExtract::run()
{
    Context& ctx = Context::instance();

    ASSERT( ctx.argc() == 2 );

    MarsRequest r("retrieve");
    r.setValue("class","od");
    r.setValue("stream","oper");
    r.setValue("expver","0001");
    r.setValue("type","an");
    r.setValue("levtype","pl");
    r.setValue("date","20160404");
    r.setValue("time","1200");
    r.setValue("param","131.128");
    r.setValue("step","0");
    r.setValue("levelist","1000");

    MarsRequest e("environ");

    MarsTask task(r,e);

    fdb5::Retriever retriever(task);

    eckit::ScopedPtr<DataHandle> dh ( retriever.retrieve() );

    eckit::FileHandle out( ctx.argv(1));

    dh->saveInto(out);
}


int main(int argc, char **argv)
{
    FdbExtract app(argc,argv);
    return app.start();
}

