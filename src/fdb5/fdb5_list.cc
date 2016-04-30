/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Context.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/FDBTool.h"
#include "fdb5/Index.h"
#include "fdb5/TocHandler.h"
#include "fdb5/Schema.h"
#include "fdb5/Rule.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public FDBTool {

    virtual void run();

public:

    FDBList(int argc, char **argv) : FDBTool(argc, argv) {}

};

class ListVisitor : public EntryVisitor {
 public:
    ListVisitor(const Key& key, const fdb5::Schema& schema): key_(key), schema_(schema) {}

private:
    virtual void visit(const Index& index,
                       const std::string &indexFingerprint,
                       const std::string &fieldFingerprint,
                       const eckit::PathName &path,
                       eckit::Offset offset,
                       eckit::Length length);

    const Key& key_;
    const fdb5::Schema& schema_;
};

void ListVisitor::visit(const Index& index,
                        const std::string &indexFingerprint,
                        const std::string &fieldFingerprint,
                        const eckit::PathName &path,
                        eckit::Offset offset,
                        eckit::Length length) {
    std::vector<Key> keys;
    keys.push_back(key_);
    keys.push_back(index.key());

    Key field(fieldFingerprint, schema_.ruleFor(keys));

    std::cout << key_ << index.key() << field << std::endl;

//    Log::info() << dbKeyStr_ << ":" << unique << " "
//                << path << " " << offset << " " << length
//                << std::endl;

}

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
        Key key = handler.databaseKey();
        Log::info() << "Database key " << key << std::endl;

        fdb5::Schema schema(path / "schema");

        std::vector<Index *> indexes = handler.loadIndexes();
        ListVisitor visitor(key, schema);

        for (std::vector<Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
            (*i)->entries(visitor);
        }

        handler.freeIndexes(indexes);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBList app(argc, argv);
    return app.start();
}

