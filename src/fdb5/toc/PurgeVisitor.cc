/*
 * (C) Copyright 1996-2017 ECMWF.
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



//----------------------------------------------------------------------------------------------------------------------

PurgeVisitor::PurgeVisitor(const eckit::PathName &directory) :
    ReportVisitor(directory) {
}

void PurgeVisitor::report(std::ostream &out) const {

    IndexStatistics total = indexStatistics();

    out << std::endl;
    out << "Index Report:" << std::endl;
    for (std::map<const Index *, IndexStatistics>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        out << "    Index " << *(i->first) << std::endl;
        i->second.report(out, "          ");
    }

    size_t indexToDelete = 0;
    out << std::endl;
    out << "Number of reachable fields per index file:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = indexUsage_.begin(); i != indexUsage_.end(); ++i) {
        out << "    " << i->first << ": " << eckit::BigNum(i->second) << std::endl;
        if (i->second == 0) {
            indexToDelete++;
        }
    }

    size_t dataToDelete = 0;
    out << std::endl;
    out << "Number of reachable fields per data file:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        out << "    " << i->first << ": " << eckit::BigNum(i->second) << std::endl;
        if (i->second == 0) {
            dataToDelete++;
        }
    }

    out << std::endl;
    size_t cnt = 0;
    out << "Unreferenced owned data files:" << std::endl;
    for (std::map<eckit::PathName, size_t>::const_iterator i = dataUsage_.begin(); i != dataUsage_.end(); ++i) {
        if (i->second == 0) {
            if (i->first.dirName().sameAs(directory_)) {
                out << "    " << i->first << std::endl;
                cnt++;
            }
        }
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
        }
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
        }
    }
    if (!cnt) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
}

void PurgeVisitor::purge(std::ostream& out) const {

    for (std::map<const fdb5::Index *, fdb5::IndexStatistics>::const_iterator i = indexStats_.begin();
            i != indexStats_.end(); ++i) {

        const fdb5::IndexStatistics &stats = i->second;

        if (stats.fieldsCount_ == stats.duplicatesCount_) {
            out << "Removing: " << *(i->first) << std::endl;
            fdb5::TocHandler handler(directory_);
            handler.writeClearRecord(*(*i).first);
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
