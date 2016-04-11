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

#include "fdb5/Rules.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

class FdbRules : public eckit::Tool {
    virtual void run();
public:
    FdbRules(int argc,char **argv): Tool(argc,argv) {}
};

void FdbRules::run()
{
    Context& ctx = Context::instance();

    ASSERT( ctx.argc() == 2 );

    Rules rules;

    rules.load(ctx.argv(1));

    rules.dump(std::cout);
}


int main(int argc, char **argv)
{
    FdbRules app(argc,argv);
    return app.start();
}

