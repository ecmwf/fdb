/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/ReportVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------



//----------------------------------------------------------------------------------------------------------------------

ReportVisitor::ReportVisitor(const eckit::PathName &directory) :
    directory_(directory) {
}

ReportVisitor::~ReportVisitor() {
}

void ReportVisitor::visit(const Index &index,
                         const std::string &indexFingerprint,
                         const std::string &fieldFingerprint,
                         const Field &field) {

    Statistics &stats = indexStats_[&index];

    ++stats.fields_;
    stats.fieldsSize_ += field.length();

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

Statistics ReportVisitor::totals() const {
    Statistics total;
    for (std::map<const Index *, Statistics>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        total += i->second;
    }
    return total;
}

void ReportVisitor::report(std::ostream &out) const {

    Statistics total = totals();

    out << std::endl;
    out << "Index Report:" << std::endl;
    for (std::map<const Index *, Statistics>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
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

    out << "   " << eckit::Plural(total.fields_, "field") << " referenced"
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

    out << "   "  << eckit::Bytes(total.fieldsSize_) << " referenced"
        << " (" << eckit::Bytes(total.fieldsSize_ - total.duplicatesSize_) << " accessible, "
        << eckit::Bytes(total.duplicatesSize_)
        << " duplicated)" << std::endl;
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
