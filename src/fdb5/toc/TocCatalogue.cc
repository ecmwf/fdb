/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <dirent.h>
#include <fcntl.h>
#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocWipeVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocCatalogue::TocCatalogue(const Key& key, const fdb5::Config& config) :
    TocCatalogue(key, CatalogueRootManager(config).directory(key), config) {}

TocCatalogue::TocCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config) :
    Catalogue(key, tocPath.controlIdentifiers_, config),
    TocHandler(tocPath.directory_, config) {}

TocCatalogue::TocCatalogue(const eckit::PathName& directory, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config) :
    Catalogue(Key(), controlIdentifiers, config),
    TocHandler(directory, config) {
    // Read the real DB key into the DB base object
    dbKey_ = databaseKey();
}

bool TocCatalogue::exists() const {
    return TocHandler::exists();
}

const std::string TocCatalogue::DUMP_PARAM_WALKSUBTOC = "walk";

void TocCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    bool walkSubToc = false;
    conf.get(DUMP_PARAM_WALKSUBTOC, walkSubToc);

    TocHandler::dump(out, simple, walkSubToc);
}

eckit::URI TocCatalogue::uri() const {
    return eckit::URI(TocEngine::typeName(), basePath());
}

const Schema& TocCatalogue::schema() const {
    return schema_;
}

const eckit::PathName& TocCatalogue::basePath() const {
    return directory_;
}

std::vector<PathName> TocCatalogue::metadataPaths() const {

    std::vector<PathName> paths(subTocPaths());

    paths.emplace_back(schemaPath());
    paths.emplace_back(tocPath());

    std::vector<PathName>&& lpaths(lockfilePaths());
    paths.insert(paths.end(), lpaths.begin(), lpaths.end());

    return paths;
}

void TocCatalogue::visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) {

    std::vector<Index> all = indexes(sorted);

    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this, store)) {
        if (visitor.visitIndexes()) {
            for (const Index& idx : all) {
                if (visitor.visitEntries()) {
                    idx.entries(visitor); // contains visitIndex
                } else {
                    visitor.visitIndex(idx);
                }
            }
        }

        visitor.catalogueComplete(*this);
    }

}

void TocCatalogue::loadSchema() {
    Timer timer("TocCatalogue::loadSchema()", Log::debug<LibFdb5>());
    schema_.load( schemaPath() );
}

StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
    return new TocStatsReportVisitor(*this);
}

PurgeVisitor *TocCatalogue::purgeVisitor(const Store& store) const {
    return new TocPurgeVisitor(*this, store);
}

WipeVisitor* TocCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
    return new TocWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
}

void TocCatalogue::maskIndexEntry(const Index &index) const {
    TocHandler handler(basePath());
    handler.writeClearRecord(index);
}

std::vector<Index> TocCatalogue::indexes(bool sorted) const {
    return loadIndexes(sorted);
}

void TocCatalogue::allMasked(std::set<std::pair<URI, Offset>>& metadata,
                      std::set<URI>& data) const {
    enumerateMasked(metadata, data);
}

std::string TocCatalogue::type() const
{
    return TocCatalogue::catalogueTypeName();
}

void TocCatalogue::checkUID() const {
    TocHandler::checkUID();
}

void TocCatalogue::remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit) path.rmdir(false);
    } else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit) path.unlink(false);
    }
}

void TocCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    TocHandler::control(action, identifiers);
}

bool TocCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
    return Catalogue::enabled(controlIdentifier) && TocHandler::enabled(controlIdentifier);
}

bool TocCatalogue::canMoveTo(const eckit::URI& dest) const {
    
    DIR* dirp = ::opendir(currentDirectory().asString().c_str());
    struct dirent* dp;
    while ((dp = readdir(dirp)) != NULL) {
        if (strstr( dp->d_name, ".index")) {
            eckit::PathName src_ = directory_ / dp->d_name;
            int fd = ::open(src_.asString().c_str(), O_RDWR);
            if(::flock(fd, LOCK_EX)) {
                std::stringstream ss;
                ss << "Index file " << dp->d_name << " is locked";
                throw eckit::UserError(ss.str(), Here());
            }
        }
    }
    closedir(dirp);

    if (dest.scheme().empty() || dest.scheme() == "toc" || dest.scheme() == "file" || dest.scheme() == "unix") {
        eckit::PathName destPath = dest.path();
        for (const eckit::PathName& root: CatalogueRootManager(config_).canArchiveRoots(dbKey_)) {
            if (root.sameAs(destPath)) {
                eckit::PathName dest_db = destPath / currentDirectory().baseName(true);

                if(dest_db.exists()) {
                    std::stringstream ss;
                    ss << "Target folder already exist!" << std::endl;
                    throw UserError(ss.str(), Here());
                }
                return true;
            }
        }
        std::stringstream ss;
        ss << "Destination " << dest << " cannot be uses to archive a DB with key: " << dbKey_ << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    std::stringstream ss;
    ss << "Destination " << dest << " not supported." << std::endl;
    throw eckit::UserError(ss.str(), Here());
}


void TocCatalogue::moveTo(const eckit::URI& dest) {
    eckit::PathName destPath = dest.path();
    for (const eckit::PathName& root: CatalogueRootManager(config_).canArchiveRoots(dbKey_)) {
        if (root.sameAs(destPath)) {
            eckit::PathName dest_db = destPath / currentDirectory().baseName(true);

            if(!dest_db.exists()) {
                dest_db.mkdir();
            }
            
            eckit::ThreadPool pool(currentDirectory().asString(), 4);

            DIR* dirp = ::opendir(currentDirectory().asString().c_str());
            struct dirent* dp;
            while ((dp = readdir(dirp)) != NULL) {
                if (strstr( dp->d_name, ".index") ||
                    strstr( dp->d_name, "toc.") ||
                    strstr( dp->d_name, "schema")) {

                    pool.push(new FileCopy(currentDirectory().path(), dest_db, dp->d_name));
                }
            }
            closedir(dirp);

            pool.wait();
            pool.push(new FileCopy(currentDirectory().path(), dest_db, "toc"));
            pool.wait();
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
