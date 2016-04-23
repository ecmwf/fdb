/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/runtime/Tool.h"
#include "eckit/runtime/Context.h"

#include "fdb5/Schema.h"
#include "fdb5/MasterConfig.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

class FdbSchema : public eckit::Tool {
    virtual void run();
public:
    FdbSchema(int argc,char **argv): Tool(argc,argv) {}
};

void FdbSchema::run()
{
    Context& ctx = Context::instance();

    ASSERT( ctx.argc() == 2 );

    fdb5::Schema rules;

    rules.load(ctx.argc() > 1 ? ctx.argv(1) : MasterConfig::instance().schemaPath());

    rules.dump(std::cout);
}


int main(int argc, char **argv)
{
    FdbSchema app(argc,argv);
    return app.start();
}

