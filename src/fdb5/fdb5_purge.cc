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
#include "eckit/log/Plural.h"

#include "fdb5/Index.h"
#include "fdb5/Key.h"
#include "fdb5/TocClearIndex.h"
#include "fdb5/TocReverseIndexes.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

struct Stats {

    Stats() : totalFields(0), duplicates(0), totalSize(0), duplicatesSize(0) {}

    size_t totalFields;
    size_t duplicates;
    Length totalSize;
    Length duplicatesSize;

    Stats& operator+=(const Stats& rhs) {
        totalFields += rhs.totalFields;
        duplicates += rhs.duplicates;
        totalSize += rhs.totalSize;
        duplicatesSize += rhs.duplicatesSize;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& s,const Stats& x) { x.print(s); return s; }

    void print(std::ostream& out) const {
        out << "Stats:"
            << " totalFields: "  << eckit::BigNum(totalFields)
            << ", duplicates: "    << eckit::BigNum(duplicates)
            << ", totalSize: "    << eckit::Bytes(totalSize)
            << ", duplicatesSize: " << eckit::Bytes(duplicatesSize);
    }

};

//----------------------------------------------------------------------------------------------------------------------

struct PurgeVisitor : public EntryVisitor {

    PurgeVisitor(const eckit::PathName& dir) : dir_(dir) {}

    virtual void visit(const std::string& index,
                       const std::string& key,
                       const eckit::PathName& path,
                       eckit::Offset offset,
                       eckit::Length length)
    {
        Stats& stats = indexStats_[current_];

        ++(stats.totalFields);
        stats.totalSize += length;

        allDataFiles_.insert(path);

        std::string indexKey = index + key;
        if(active_.find(indexKey) == active_.end()) {
            active_.insert(indexKey);
            activeDataFiles_.insert(path);
        }
        else {
            ++(stats.duplicates);
            stats.duplicatesSize += length;
        }
    }

    void currentIndex(const eckit::PathName& path) { current_ = path; }

    Stats totals() const {
        Stats total;
        for(std::map<eckit::PathName, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
            total += i->second;
        }
        return total;
    }

    eckit::PathName dir_;
    eckit::PathName current_; //< current Index being scanned

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;

    std::set<std::string> active_;

    std::map<eckit::PathName, Stats> indexStats_;


    friend std::ostream& operator<<(std::ostream& s,const PurgeVisitor& x) { x.print(s); return s; }

    void print(std::ostream& out) const {
        out << "PurgeVisitor(indexStats=" << indexStats_ << ")";
    }

    void report(std::ostream& out) const {

        Stats total = totals();

        out << "Index Report:" << std::endl;
        for(std::map<eckit::PathName, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
            out << "    Index " << i->first << " " << i->second << std::endl;
        }

        out << "Summary:" << std::endl;
        out << "   " << eckit::Plural(total.totalFields, "field") << " referenced" << std::endl;
        out << "   " << eckit::Plural(active_.size(), "field") << " active" << std::endl;
        out << "   " << eckit::Plural(total.duplicates, "field") << " duplicated" << std::endl;
        out << "   " << eckit::Plural(allDataFiles_.size(), "data file") << " referenced" << std::endl;
        out << "   " << eckit::Plural(activeDataFiles_.size(), "data file") << " active" << std::endl;
        out << "   " << eckit::Bytes(total.totalSize) << " referenced" << std::endl;
        out << "   " << eckit::Bytes(total.duplicatesSize) << " duplicated" << std::endl;
        out << std::endl;

        out << "Data file(s) than can deleted:" << std::endl;
        for(std::set<eckit::PathName>::const_iterator i = allDataFiles_.begin(); i != allDataFiles_.end(); ++i) {
            if(activeDataFiles_.find(*i) == activeDataFiles_.end()) {
                out << "    " << *i;
                if(!i->dirName().sameAs(dir_)) {
                    out << " -> Not owned by FDB, it will not be deleted.";
                }
                out << std::endl;
            }
        }
    }

};

//----------------------------------------------------------------------------------------------------------------------

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


        PurgeVisitor visitor(path);

        for(std::vector<eckit::PathName>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {

            Log::info() << "Index path " << *i << std::endl;

            Key dummy;
            eckit::ScopedPtr<Index> index ( Index::create(dummy, *i, Index::READ) );

            visitor.currentIndex(*i);

            index->entries(visitor);
        }

        visitor.report(Log::info());

        for(std::map<eckit::PathName, Stats>::const_iterator i = visitor.indexStats_.begin();
            i != visitor.indexStats_.end(); ++i) {

            const Stats& stats = i->second;

            if(stats.totalFields == stats.duplicates) {
                Log::info() << "Index is inactive: " << i->first << std::endl;
                TocClearIndex clear(path, i->first);
            }
        }

    }

}


int main(int argc, char **argv)
{
    FDBList app(argc,argv);
    return app.start();
}
