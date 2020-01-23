/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocPurgeVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocPurgeVisitor::TocPurgeVisitor(const TocCatalogue& catalogue, const Store& store) :
    PurgeVisitor(),
    TocStatsReportVisitor(catalogue, false),
    store_(store) {}

TocPurgeVisitor::~TocPurgeVisitor() {}

bool TocPurgeVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    std::set<std::pair<URI, Offset>> metadata;
    std::set<URI> data;

    catalogue.allMasked(metadata, data);

    for (const auto& entry : metadata) {
        const PathName& path = entry.first.path();

        allIndexFiles_.insert(path);
        indexUsage_[path] += 0;
    }

    for (const auto& path : data) {
        allDataFiles_.insert(path.path());
        dataUsage_[path.path()] += 0;
    }

    return true;
}


void TocPurgeVisitor::report(std::ostream& out) const {

    const eckit::PathName& directory(((TocCatalogue*) currentCatalogue_)->basePath());

    out << std::endl;
    out << "Index Report:" << std::endl;
    for (const auto& it : indexStats_) { // <Index, IndexStats>
        out << "    Index " << it.first << std::endl;
        it.second.report(out, "          ");
    }

    size_t indexToDelete = 0;
    out << std::endl;
    out << "Number of reachable fields per index file:" << std::endl;
    for (const auto& it : indexUsage_) { // <std::string, size_t>
        out << "    " << it.first << ": " << eckit::BigNum(it.second) << std::endl;
        if (it.second == 0) {
            indexToDelete++;
        }
    }

    size_t dataToDelete = 0;
    out << std::endl;
    out << "Number of reachable fields per data file:" << std::endl;
    for (const auto& it : dataUsage_) { // <std::string, size_t>
        out << "    " << it.first << ": " << eckit::BigNum(it.second) << std::endl;
        if (it.second == 0) {
            dataToDelete++;
        }
    }

    out << std::endl;
    size_t cnt = 0;
    out << "Unreferenced owned data files:" << std::endl;
    for (const auto& it : dataUsage_) { // <std::string, size_t>
        if (it.second == 0) {
            if (eckit::PathName(it.first).dirName().sameAs(directory)) {
                out << "    " << it.first << std::endl;
                cnt++;
            }
        }
    }
    if (!cnt) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
    size_t cnt2 = 0;
    out << "Unreferenced adopted data files:" << std::endl;
    for (const auto& it : dataUsage_) { // <std::string, size_t>
        if (it.second == 0) {
            if (!eckit::PathName(it.first).dirName().sameAs(directory)) {
                out << "    " << it.first << std::endl;
                cnt2++;
            }
        }
    }
    if (!cnt2) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
    size_t cnt3 = 0;
    out << "Index files to be deleted:" << std::endl;
    for (const auto& it : indexUsage_) { // <std::string, size_t>
        if (it.second == 0) {
            out << "    " << it.first << std::endl;
            cnt3++;
        }
    }
    if (!cnt3) {
        out << "    - NONE -" << std::endl;
    }

    out << std::endl;
}

void TocPurgeVisitor::purge(std::ostream& out, bool porcelain, bool doit) const {

    std::ostream& logAlways(out);
    std::ostream& logVerbose(porcelain ? Log::debug<LibFdb5>() : out);

    currentCatalogue_->checkUID();

    const TocCatalogue* currentCatalogue = dynamic_cast<const TocCatalogue*>(currentCatalogue_);
    ASSERT(currentCatalogue);

    const eckit::PathName directory((currentCatalogue)->basePath());

    for (const auto& it : indexStats_) { // <Index, IndexStats>

        const fdb5::IndexStats& stats = it.second;

        if (stats.fieldsCount() == stats.duplicatesCount()) {
            logVerbose << "Removing: " << it.first << std::endl;
            if (doit) {
                fdb5::TocHandler handler(directory);
                handler.writeClearRecord(it.first);
            }
        }
    }

    for (const auto& it : dataUsage_) { // <std::string, size_t>
        if (it.second == 0) {
            eckit::PathName path(it.first);
            if (path.dirName().sameAs(directory)) {
                store_.remove(eckit::URI(store_.type(), path), logAlways, logVerbose, doit);
            }
        }
    }

    for (const auto& it : indexUsage_) { // <std::string, size_t>
        if (it.second == 0) {
            eckit::PathName path(it.first);
            if (path.dirName().sameAs(directory)) {
                currentCatalogue->remove(path, logAlways, logVerbose, doit);
            }
       }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
