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

#include "fdb5/LibFdb5.h"
#include "fdb5/toc/TocHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocPurgeVisitor::TocPurgeVisitor(const TocCatalogue& catalogue, const Store& store) :
    PurgeVisitor(), TocStatsReportVisitor(catalogue, false), store_(store) {}

TocPurgeVisitor::~TocPurgeVisitor() {}

bool TocPurgeVisitor::visitDatabase(const Catalogue& catalogue) {

    std::set<std::pair<URI, Offset>> metadata;
    std::set<URI> data;

    catalogue.allMasked(metadata, data);

    for (const auto& entry : metadata) {
        const PathName& path = entry.first.path();

        allIndexFiles_.insert(path);
        indexUsage_[path] += 0;
    }

    for (const auto& uri : data) {
        if (!store_.uriBelongs(uri)) {
            Log::error() << "Catalogue is pointing to data files that do not belong to the store." << std::endl;
            Log::error() << "Configured Store URI: " << store_.uri().asString() << std::endl;
            Log::error() << "Pointed Store unit URI: " << uri.asString() << std::endl;
            Log::error() << "This may occur when purging an overlayed FDB, which is not supported." << std::endl;
            NOTIMP;
        }
        allDataFiles_.insert(uri.path());
        dataUsage_[uri.path()] += 0;
    }

    return true;
}

void TocPurgeVisitor::gatherAuxiliaryURIs() {
    for (const auto& it : dataUsage_) {  // <std::string, size_t>

        // Check if .data file is deletable
        bool deletable = false;
        if (it.second == 0) {
            eckit::PathName path(it.first);
            if (store_.uriBelongs(eckit::URI(store_.type(), path))) {
                deletable = true;
            }
        }

        // Add auxiliary files to the corresponding set
        eckit::URI uri(store_.type(), eckit::PathName(it.first));
        for (const auto& auxURI : store_.getAuxiliaryURIs(uri)) {
            if (!store_.auxiliaryURIExists(auxURI)) {
                continue;
            }
            // Todo: in future can we just use URIs, not paths?
            eckit::PathName auxPath = auxURI.path();
            if (deletable) {
                deleteAuxFiles_.insert(auxPath);
            }
            else {
                keepAuxFiles_.insert(auxPath);
            }
        }
    }
}

void TocPurgeVisitor::report(std::ostream& out) const {
    const TocCatalogue* cat = dynamic_cast<const TocCatalogue*>(currentCatalogue_);
    const eckit::PathName& directory(cat->basePath());

    out << std::endl;
    out << "Index Report:" << std::endl;
    for (const auto& it : indexStats_) {  // <Index, IndexStats>
        out << "    Index " << it.first << std::endl;
        it.second.report(out, "          ");
    }

    out << std::endl;
    size_t cnt = 0;
    out << "Unreferenced owned data files:" << std::endl;
    for (const auto& it : dataUsage_) {  // <std::string, size_t>
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
    for (const auto& it : dataUsage_) {  // <std::string, size_t>
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

    // Auxiliary files
    out << "Auxiliary files to be deleted:" << std::endl;
    for (const auto& it : deleteAuxFiles_) {
        out << "    " << it << std::endl;
    }
    if (deleteAuxFiles_.empty()) {
        out << "    - NONE -" << std::endl;
    }
    out << std::endl;

    out << "Auxiliary files to be kept:" << std::endl;
    for (const auto& it : keepAuxFiles_) {
        out << "    " << it << std::endl;
    }
    if (keepAuxFiles_.empty()) {
        out << "    - NONE -" << std::endl;
    }
    out << std::endl;

    size_t cnt3 = 0;
    out << "Index files to be deleted:" << std::endl;
    for (const auto& it : indexUsage_) {  // <std::string, size_t>
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

    for (const auto& it : indexStats_) {  // <Index, IndexStats>

        const fdb5::IndexStats& stats = it.second;

        if (stats.fieldsCount() == stats.duplicatesCount()) {
            logVerbose << "Removing: " << it.first << std::endl;
            if (doit) {
                fdb5::TocHandler handler(directory, Config().expandConfig());
                handler.writeClearRecord(it.first);
            }
        }
    }

    for (const auto& it : dataUsage_) {  // <std::string, size_t>
        if (it.second == 0) {
            eckit::PathName path(it.first);
            if (path.dirName().sameAs(directory)) {
                store_.remove(eckit::URI(store_.type(), path), logAlways, logVerbose, doit);
            }
        }
    }

    for (const auto& path : deleteAuxFiles_) {
        if (path.dirName().sameAs(directory) && keepAuxFiles_.find(path) == keepAuxFiles_.end()) {
            store_.remove(eckit::URI(store_.type(), path), logAlways, logVerbose, doit);
        }
    }

    for (const auto& it : indexUsage_) {  // <std::string, size_t>
        if (it.second == 0) {
            eckit::PathName path(it.first);
            if (path.dirName().sameAs(directory)) {
                currentCatalogue->remove(path, logAlways, logVerbose, doit);
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
