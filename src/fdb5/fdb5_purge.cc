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
#include "eckit/log/Bytes.h"
#include "eckit/log/BigNum.h"

#include "fdb5/Key.h"
#include "fdb5/TocReverseIndexes.h"
#include "fdb5/Index.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

class FDBList : public eckit::Tool {
    virtual void run();
public:
    FDBList(int argc,char **argv): Tool(argc,argv) {}
};

void FDBList::run()
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


        struct PurgeVisitor : public EntryVisitor {

            PurgeVisitor() :
                totalCount_(0),
                totalSize_(0),
                uniqueSize_(0),
                duplicateCount_(0),
                duplicateSize_(0)
            {
            }

            virtual void visit(const std::string& index,
                               const std::string& key,
                               const eckit::PathName& path,
                               eckit::Offset offset,
                               eckit::Length length)
            {
                ++totalCount_;
                totalSize_ += length;

                std::string indexKey = index + key;
                if(active_.find(indexKey) == active_.end()) {
                    active_.insert( make_pair(indexKey, Index::Field(path, offset, length)));
                    uniqueSize_ += length;
                }
                else {
                    ++duplicates_[current_];
                    duplicateSize_ += length;
                }
            }

            size_t totalCount_;
            Length totalSize_;
            Length uniqueSize_;
            size_t duplicateCount_;
            Length duplicateSize_;

            eckit::PathName current_;

            std::map<std::string, Index::Field> active_;

            std::map<eckit::PathName, size_t> duplicates_;

            void currentIndex(const eckit::PathName& path) { current_ = path; }
        };

        PurgeVisitor visitor;

        for(std::vector<eckit::PathName>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {

            Log::info() << "Index path " << *i << std::endl;

            Key dummy;
            eckit::ScopedPtr<Index> index ( Index::create(dummy, *i, Index::READ) );

            visitor.currentIndex(*i);

            index->entries(visitor);
        }

        Log::info() << "FDB Totals:"              << std::endl
                    << "    fields count      : " << eckit::BigNum(visitor.totalCount_) << std::endl
                    << "    unique fields     : " << eckit::BigNum(visitor.active_.size()) << std::endl
                    << "    unique size       : " << eckit::Bytes(visitor.uniqueSize_) << std::endl
                    << "    duplicates fields : " << eckit::BigNum(visitor.duplicateCount_) << std::endl
                    << "    duplicates size   : " << eckit::Bytes(visitor.duplicateSize_) << std::endl
                    << "    total size        : " << eckit::Bytes(visitor.totalSize_) << std::endl;
    }

}


int main(int argc, char **argv)
{
    FDBList app(argc,argv);
    return app.start();
}
