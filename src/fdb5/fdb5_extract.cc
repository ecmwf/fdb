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
#include "eckit/io/FileHandle.h"

#include "marslib/MarsTask.h"

#include "fdb5/Retriever.h"
#include "fdb5/GribDecoder.h"

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

    fdb5::GribDecoder decoder;

    MarsRequest r = decoder.gribToRequest(ctx.argv(1));
    std::cout << r << std::endl;

    // MarsRequest r("retrieve");
    // r.setValue("class","od");
    // r.setValue("stream","oper");
    // r.setValue("expver","0001");
    // r.setValue("type","an");
    // r.setValue("levtype","pl");
    // r.setValue("date","20160412");
    // r.setValue("time","1200");
    // r.setValue("domain","g");

    // vector<string> params;
    // params.push_back( "131" );
    // params.push_back( "138" );
    // // params.push_back( "129" );
    // r.setValues("param",params);

    // r.setValue("step","0");

    // vector<string> levels;
    // levels.push_back( "850" );
    // levels.push_back( "400" );
    // levels.push_back( "300" );
    // r.setValues("levelist",levels);

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

