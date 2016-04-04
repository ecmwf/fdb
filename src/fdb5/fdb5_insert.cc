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

#include "fdb5/GribArchiver.h"

using namespace eckit;

class FdbInsert : public eckit::Tool {
    virtual void run();
public:
    FdbInsert(int argc,char **argv): Tool(argc,argv) {}
};

void FdbInsert::run()
{
    Context& ctx = Context::instance();

    fdb5::GribArchiver archiver;

    for(int i = 1; i < ctx.argc(); i++)
    {
        eckit::PathName path(ctx.argv(i));

        std::cout << path << std::endl;
        
        try 
        {
            std::auto_ptr<DataHandle> dh ( path.fileHandle() );
            archiver.archive( *dh );
        }
        catch(Exception& e)
        {
            std::cout << e.what() << std::endl;
        }        
    }
}


int main(int argc, char **argv)
{
    FdbInsert app(argc,argv);
    app.start();
    return 0;
}

