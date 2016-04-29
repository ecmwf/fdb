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
#include "fdb5/TocHandler.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

class FDBList : public eckit::Tool {
    virtual void run();
public:
    FDBList(int argc, char **argv): Tool(argc, argv) {}
};

void FDBList::run() {
    Context &ctx = Context::instance();

    for (int i = 1; i < ctx.argc(); i++) {

        eckit::PathName path(ctx.argv(i));

        if (!path.isDir()) {
            path = path.dirName();
        }

        path = path.realName();

        Log::info() << "Listing " << path << std::endl;


        fdb5::TocHandler handler(path);

        std::vector<Index *> indexes = handler.loadIndexes();

        for (std::vector<Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
            Log::info() << "Index path " << (*i)->path() << std::endl;
            (*i)->list(Log::info());
        }

        handler.freeIndexes(indexes);

    }

}


int main(int argc, char **argv) {
    FDBList app(argc, argv);
    return app.start();
}

