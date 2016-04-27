/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/PurgeVisitor.h"

#include "eckit/log/Log.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Plural.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/Index.h"
#include "fdb5/TocClearIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void Stats::print(std::ostream& out) const {
    out << "Stats:"
        << " totalFields: "  << eckit::BigNum(totalFields)
        << ", duplicates: "    << eckit::BigNum(duplicates)
        << ", totalSize: "    << eckit::Bytes(totalSize)
        << ", duplicatesSize: " << eckit::Bytes(duplicatesSize);
}

//----------------------------------------------------------------------------------------------------------------------

PurgeVisitor::PurgeVisitor(const eckit::PathName& dir) :
    dir_(dir)
{
}

void PurgeVisitor::visit(const std::string& index, const std::string& key, const eckit::PathName& path, eckit::Offset offset, eckit::Length length)
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

void PurgeVisitor::currentIndex(const eckit::PathName& path) { current_ = path; }

Stats PurgeVisitor::totals() const {
    Stats total;
    for(std::map<eckit::PathName, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        total += i->second;
    }
    return total;
}

void PurgeVisitor::print(std::ostream& out) const {
    out << "PurgeVisitor(indexStats=" << indexStats_ << ")";
}

std::vector<eckit::PathName> PurgeVisitor::filesToBeDeleted(size_t& adopted, size_t& duplicated, size_t& duplicatedAdopted) const {

    std::vector<eckit::PathName> result;

    for(std::set<eckit::PathName>::const_iterator i = allDataFiles_.begin(); i != allDataFiles_.end(); ++i) {

        bool adoptedFile = (!i->dirName().sameAs(dir_));

        if(adoptedFile) { ++adopted; }

        if(activeDataFiles_.find(*i) == activeDataFiles_.end()) {
            ++duplicated;
            if(adoptedFile) {
                ++duplicatedAdopted;
            }
            else {
                result.push_back(*i);
            }
        }
    }
    return result;
}

void PurgeVisitor::report(std::ostream& out) const {

    Stats total = totals();

    out << "Index Report:" << std::endl;
    for(std::map<eckit::PathName, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        out << "    Index " << i->first << " " << i->second << std::endl;
    }

    size_t adopted = 0;
    size_t duplicated = 0;
    size_t duplicatedAdopted = 0;

    std::vector<eckit::PathName> tobeDeleted = filesToBeDeleted(adopted, duplicated, duplicatedAdopted);

    out << "Data file report:" << std::endl;
    for(std::vector<eckit::PathName>::const_iterator i = tobeDeleted.begin(); i != tobeDeleted.end(); ++i) {
        out << "    " << *i << std::endl;
    }

    out << "Summary:" << std::endl;

    out << "   " << eckit::Plural(total.totalFields, "field") << " referenced"
        << " ("   << eckit::Plural(active_.size(), "field") << " active"
        << ", "  << eckit::Plural(total.duplicates, "field") << " duplicated)" << std::endl;

    out << "   " << eckit::Plural(allDataFiles_.size(), "data file") << " referenced"
        << " of which "   << adopted << " adopted"
        << " ("   << activeDataFiles_.size() << " active"
        << ", "  << duplicated << " duplicated"
        << " of which "  << duplicatedAdopted << " adopted)" << std::endl;

    out << "   " << eckit::Plural(tobeDeleted.size(), "file") << " to delete" << std::endl;

    out << "   "  << eckit::Bytes(total.totalSize) << " referenced"
        << " (" << eckit::Bytes(total.duplicatesSize) << " duplicated)" << std::endl;
}

void PurgeVisitor::purge(bool doit) const
{
    // clear Toc Index

    for(std::map<eckit::PathName, Stats>::const_iterator i = indexStats_.begin();
        i != indexStats_.end(); ++i) {

        const Stats& stats = i->second;

        if(stats.totalFields == stats.duplicates) {
            eckit::Log::info() << "Index to remove: " << i->first << std::endl;

            if(doit) { TocClearIndex clear(dir_, i->first); }

            Key dummy;
            eckit::ScopedPtr<Index> index ( Index::create(dummy, i->first, Index::READ) );
            index->deleteFiles(doit);
        }
    }

    // delete data files

    size_t adopted = 0;
    size_t duplicated = 0;
    size_t duplicatedAdopted = 0;
    std::vector<eckit::PathName> tobeDeleted = filesToBeDeleted(adopted, duplicated, duplicatedAdopted);

    for(std::vector<eckit::PathName>::const_iterator i = tobeDeleted.begin(); i != tobeDeleted.end(); ++i) {
        eckit::Log::info() << "File to remove " << *i << std::endl;
        if(doit) { i->unlink(); }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
