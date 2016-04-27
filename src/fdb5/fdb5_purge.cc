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
#include "eckit/filesystem/PathName.h"

#include "fdb5/Index.h"
#include "fdb5/Key.h"
#include "fdb5/TocReverseIndexes.h"
#include "fdb5/PurgeVisitor.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class FDBPurge : public eckit::Tool {
    virtual void run();
public:
    FDBPurge(int argc,char **argv): Tool(argc,argv) {}
};

void FDBPurge::run()
{
    Context& ctx = Context::instance();

    for (int i = 1; i < ctx.argc(); i++) {

        eckit::PathName path(ctx.argv(i));

        if(!path.isDir()) {
            path = path.dirName();
        }

        path = path.realName();

        Log::info() << "Listing " << path << std::endl;

        fdb5::TocReverseIndexes toc(path);

        std::vector<eckit::PathName> indexes = toc.indexes();


        PurgeVisitor visitor(path);

        for(std::vector<eckit::PathName>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {

            Log::info() << "Index path " << *i << std::endl;

            Key dummy;
            eckit::ScopedPtr<Index> index ( Index::create(dummy, *i, Index::READ) );

            visitor.currentIndex(*i);

            index->entries(visitor);
        }

        visitor.report(Log::info());

        visitor.purge(false);
    }
}

//----------------------------------------------------------------------------------------------------------------------
int main(int argc, char **argv)
{
    FDBPurge app(argc,argv);
    return app.start();
}
