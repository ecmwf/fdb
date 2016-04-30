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

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/TocIndex.h"
#include "fdb5/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void Stats::print(std::ostream &out) const {
    out << "Stats:"
        << " number of fields: "  << eckit::BigNum(totalFields_)
        << ", number of duplicates: "    << eckit::BigNum(duplicates_)
        << ", total size: "    << eckit::Bytes(totalSize_)
        << ", size pf dumplicates: " << eckit::Bytes(duplicatesSize_);
}

//----------------------------------------------------------------------------------------------------------------------

PurgeVisitor::PurgeVisitor(const eckit::PathName &directory) :
    directory_(directory) {
}

void PurgeVisitor::visit(const Index &index,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint,
                         const eckit::PathName &path,
                         eckit::Offset offset,
                         eckit::Length length) {

    Stats &stats = indexStats_[&index];

    ++stats.totalFields_;
    stats.totalSize_ += length;

    allDataFiles_.insert(path);
    indexUsage_[index.path()]++;
    dataUsage_[path]++;

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);
        activeDataFiles_.insert(path);
    } else {
        ++stats.duplicates_;
        stats.duplicatesSize_ += length;
        indexUsage_[index.path()]--;
        dataUsage_[path]--;
    }
}

Stats PurgeVisitor::totals() const {
    Stats total;
    for (std::map<const Index *, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        total += i->second;
    }
    return total;
}

void PurgeVisitor::report(std::ostream &out) const {

    Stats total = totals();

    out << std::endl;
    out << "Index Report:" << std::endl;
    for (std::map<const Index *, Stats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        out << "    Index " << *(i->first) << std::endl
            << "          " << i->second << std::endl;
    }

    size_t indexToDelete = 0;
    out << std::endl;
    out << "Number of accessible fields per index file:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        out << "    " << i->first << ": " << eckit::BigNum(i->second) << std::endl;
        if (i->second == 0) {
            indexToDelete++;
        }
    }

    size_t dataToDelete = 0;
    out << std::endl;
    out << "Number of accessible fields per data file:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        out << "    " << i->first << ": " << eckit::BigNum(i->second) << std::endl;
        if (i->second == 0) {
            dataToDelete++;
        }
    }

    out << std::endl;
    out << "Data files to be deleted:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            out << "    " << i->first << std::endl;
        };
    }

    out << std::endl;
    out << "Index files to be deleted:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        if (i->second == 0) {
            out << "    " << i->first << std::endl;
        };
    }

    size_t adopted = 0;
    size_t duplicated = 0;
    size_t duplicatedAdopted = 0;

    for (std::set<eckit::PathName>::const_iterator i = allDataFiles_.begin(); i != allDataFiles_.end(); ++i) {

        bool adoptedFile = (!i->dirName().sameAs(directory_));

        if (adoptedFile) {
            ++adopted;
        }

        if (activeDataFiles_.find(*i) == activeDataFiles_.end()) {
            ++duplicated;
            if (adoptedFile) {
                ++duplicatedAdopted;
            }
        }
    }

    out << std::endl;
    out << "Summary:" << std::endl;

    out << "   " << eckit::Plural(total.totalFields_, "field") << " referenced"
        << " ("   << eckit::Plural(active_.size(), "field") << " active"
        << ", "  << eckit::Plural(total.duplicates_, "field") << " duplicated)" << std::endl;

    out << "   " << eckit::Plural(allDataFiles_.size(), "data file") << " referenced"
        << " of which "   << adopted << " adopted"
        << " ("   << activeDataFiles_.size() << " active"
        << ", "  << duplicated << " duplicated"
        << " of which "  << duplicatedAdopted << " adopted)" << std::endl;

    out << "   " << eckit::Plural(dataToDelete, "data file") << " to delete" << std::endl;
    out << "   " << eckit::Plural(indexToDelete, "index file") << " to delete" << std::endl;
    out << "   " << eckit::Plural(indexToDelete, "index record") << " to clear" << std::endl;

    out << "   "  << eckit::Bytes(total.totalSize_) << " referenced"
        << " (" << eckit::Bytes(total.duplicatesSize_) << " duplicated)" << std::endl;
}

void PurgeVisitor::purge() const {


    for (std::map<const Index *, Stats>::const_iterator i = indexStats_.begin();
            i != indexStats_.end(); ++i) {

        const Stats &stats = i->second;

        if (stats.totalFields_ == stats.duplicates_) {
            eckit::Log::info() << "Removing: " << *(i->first) << std::endl;
            TocHandler handler(directory_);
            handler.writeClearRecord(*(*i).first);
        }
    }


    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            i->first.unlink();
        }
    }

    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        if (i->second == 0) {
            i->first.unlink();
        }
    }

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
