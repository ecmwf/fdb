/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/PurgeVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void Stats::print(std::ostream &out) const {
    out << "Stats:"
        << " number of fields: "  << eckit::BigNum(totalFields_)
        << ", number of duplicates: "    << eckit::BigNum(duplicates_)
        << ", number of wiped entries: "    << eckit::BigNum(wiped_)
        << ", total size: "    << eckit::Bytes(totalSize_)
        << ", size of duplicates: " << eckit::Bytes(duplicatesSize_)
        << ", size of wiped entries: " << eckit::Bytes(wipedSize_);

}

//----------------------------------------------------------------------------------------------------------------------

PurgeVisitor::PurgeVisitor(const eckit::PathName &directory) :
    directory_(directory) {
}

PurgeVisitor::~PurgeVisitor() {
    for (std::set<Index *>::iterator j = delete_.begin(); j != delete_.end(); ++j) {
        delete (*j);
    }
}

void PurgeVisitor::wipe(const std::vector<Index *> &indexes) {


    struct Wiped : public EntryVisitor {

        PurgeVisitor &owner_;

        Wiped(PurgeVisitor &owner): owner_(owner) {
        }

        void visit(const Index &index,
                   const std::string &indexFingerprint,
                   const std::string &fieldFingerprint,
                   const Field &field) {

            const eckit::PathName &path = field.path();

            owner_.allDataFiles_.insert(path);
            owner_.indexUsage_[index.path()] = 0;
            owner_.dataUsage_[path] = 0;
            owner_.wiped_.insert(index.path());

            Stats &stats = owner_.indexStats_[&index];
            ++stats.totalFields_;
            stats.totalSize_ += field.length();

            ++stats.wiped_;
            stats.wipedSize_ += field.length();
        }
    };

    Wiped w(*this);


    for (std::vector<Index *>::const_iterator j = indexes.begin(); j != indexes.end(); ++j) {
        eckit::Log::info() << "WIPE ====> " << *(*j) << std::endl;
        (*j)->wiped(true);
        (*j)->entries(w);
        delete_.insert(*j);
    }
}

void PurgeVisitor::visit(const Index &index,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint,
                         const Field &field) {

    Stats &stats = indexStats_[&index];

    ++stats.totalFields_;
    stats.totalSize_ += field.length();

    const eckit::PathName &path = field.path();

    allDataFiles_.insert(path);
    indexUsage_[index.path()]++;
    dataUsage_[path]++;

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);
        activeDataFiles_.insert(path);
    } else {
        ++stats.duplicates_;
        stats.duplicatesSize_ += field.length();
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
    size_t cnt = 0;
    out << "Data files to be deleted:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            if (i->first.dirName().sameAs(directory_)) {
                out << "    " << i->first << std::endl;
                cnt++;
            }
        };
    }
    if (!cnt) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
    cnt = 0;
    out << "Unreferenced adopted data files:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            if (!i->first.dirName().sameAs(directory_)) {
                out << "    " << i->first << std::endl;
                cnt++;
            }
        };
    }
    if (!cnt) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
    cnt = 0;
    out << "Index files to be deleted:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        if (i->second == 0) {
            out << "    " << i->first << std::endl;
            cnt++;
        };
    }
    if (!cnt) {
        out << "    - NONE -" << std::endl;
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

    out << "   " << eckit::Plural(indexUsage_.size(), "index file") << std::endl;
    out << "   " << eckit::Plural(dataUsage_.size(), "data file") << std::endl;

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
        << " (" << eckit::Bytes(total.totalSize_ - total.duplicatesSize_) << " accessible, "
        << eckit::Bytes(total.duplicatesSize_)
        << " duplicated)" << std::endl;
}

void PurgeVisitor::purge() const {


    for (std::map<const Index *, Stats>::const_iterator i = indexStats_.begin();
            i != indexStats_.end(); ++i) {

        if (!i->first->wiped()) {

            const Stats &stats = i->second;

            if (stats.totalFields_ == stats.duplicates_) {
                eckit::Log::info() << "Removing: " << *(i->first) << std::endl;
                TocHandler handler(directory_);
                handler.writeClearRecord(*(*i).first);
            }
        }
    }


    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            if (i->first.dirName().sameAs(directory_)) {
                i->first.unlink();
            }
        }
    }

    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        if (i->second == 0) {
            if (i->first.dirName().sameAs(directory_)) {
                i->first.unlink();
            }
        }
    }

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
