/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamPurgeVisitor.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Store.h"
#include "fdb5/fam/FamCatalogue.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/log/Log.h"

#include <ostream>
#include <set>
#include <string>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FamPurgeVisitor::FamPurgeVisitor(const FamCatalogue& catalogue, const Store& store) :
    FamStatsReportVisitor(catalogue), store_(store) {}

bool FamPurgeVisitor::visitDatabase(const Catalogue& catalogue) {
    ASSERT(&catalogue == currentCatalogue_);

    // Seed data referenced only by masked (logically deleted) indexes so it is purged too.
    // Kept in a dedicated set (not dataUsage_) so it never affects statistics.
    std::set<std::pair<eckit::URI, eckit::Offset>> metadata;
    std::set<eckit::URI> data;
    catalogue.allMasked(metadata, data);

    for (const auto& uri : data) {
        maskedData_.insert(uri.asRawString());
    }

    return true;
}

void FamPurgeVisitor::report(std::ostream& out) const {
    out << '\n' << "Index Report:" << '\n';
    for (const auto& [index, stats] : indexStats_) {
        out << "    Index " << index << '\n';
        stats.report(out, "          ");
    }

    out << '\n' << "Unreferenced data objects:" << '\n';
    size_t count = 0;
    for (const auto& [uri, usage] : dataUsage_) {
        if (usage == 0) {
            out << "    " << uri << '\n';
            ++count;
        }
    }
    for (const auto& uri : maskedData_) {
        if (dataUsage_.find(uri) == dataUsage_.end()) {
            out << "    " << uri << '\n';
            ++count;
        }
    }
    if (count == 0) {
        out << "    - NONE -" << '\n';
    }
    out << '\n';
}

void FamPurgeVisitor::purge(std::ostream& out, bool porcelain, bool doit) const {
    // Removes the unreferenced (superseded or masked) data objects. The stale index-map entries
    // that referenced them are intentionally retained — mirroring TocCatalogue keeping its TOC
    // records — since compacting them away is the job of reconsolidate() (NOTIMP by design).
    std::ostream& log_always(out);
    std::ostream& log_verbose(porcelain ? eckit::Log::debug<LibFdb5>() : out);

    for (const auto& [uri, usage] : dataUsage_) {
        if (usage == 0) {
            store_.remove(eckit::URI{uri}, log_always, log_verbose, doit);
        }
    }
    for (const auto& uri : maskedData_) {
        if (dataUsage_.find(uri) == dataUsage_.end()) {
            store_.remove(eckit::URI{uri}, log_always, log_verbose, doit);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
