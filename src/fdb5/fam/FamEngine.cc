/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fam/FamEngine.h"

#include "eckit/eckit.h"
#include "eckit/filesystem/LocalFileManager.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/StdDir.h"
#include "eckit/log/Log.h"
#include "eckit/os/BackTrace.h"
#include "eckit/os/Stat.h"
#include "eckit/utils/Regex.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamHandler.h"
#include "fdb5/rules/Schema.h"

#include <dirent.h>

#include <algorithm>
#include <cstring>
#include <list>
#include <ostream>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FamEngine::scan_dbs(const std::string& path, std::list<std::string>& dbs) const {
    if ((eckit::PathName(path) / "fam").exists()) {
        dbs.push_back(path);
        return;
    }

    eckit::StdDir d(path.c_str());
    if (d == nullptr) {
        // If fdb-wipe is running in parallel, it is perfectly legit for a (non-matching)
        // path to have disappeared
        if (errno == ENOENT) { return; }

        // It should not be an error if we don't have permission to read a path/DB in the
        // tree. This is a multi-user system.
        if (errno == EACCES) { return; }

        Log::error() << "opendir(" << path << ")" << Log::syserr << std::endl;
        throw FailedSystemCall("opendir");
    }

    // Once readdir_r finally gets deprecated and removed, we may need to
    // protecting readdir() as not yet guarranteed thread-safe by POSIX
    // technically it should only be needed on a per-directory basis
    // this should be a resursive mutex
    // AutoLock<Mutex> lock(mutex_);

    for (;;) {
        struct dirent* e = d.dirent();
        if (e == nullptr) { break; }

        if (e->d_name[0] == '.') {
            if (e->d_name[1] == 0 || (e->d_name[1] == '.' && e->d_name[2] == 0)) { continue; }
        }

        std::string full = path;
        if (path[path.length() - 1] != '/') { full += "/"; }
        full += e->d_name;

        bool do_stat = true;

#if defined(eckit_HAVE_DIRENT_D_TYPE)
        do_stat = false;
        if (e->d_type == DT_DIR) {
            scan_dbs(full.c_str(), dbs);
        } else if (e->d_type == DT_UNKNOWN) {
            do_stat = true;
        }
#endif
        if (do_stat) {
            eckit::Stat::Struct info;
            if (eckit::Stat::stat(full.c_str(), &info) == 0) {
                if (S_ISDIR(info.st_mode)) { scan_dbs(full.c_str(), dbs); }
            } else {
                Log::error() << "Cannot stat " << full << Log::syserr << std::endl;
            }
        }
    }
}

std::string FamEngine::name() const {
    return FamCommon::typeName;
}

std::string FamEngine::dbType() const {
    return FamCommon::typeName;
}

eckit::URI FamEngine::location(const Key& key, const Config& config) const {
    return FamCommon(config, key).uri();
}

bool FamEngine::canHandle(const eckit::URI& uri, const Config& /* config */) const {
    // if (uri.scheme() != "fam") { return false; }
    // eckit::PathName path = uri.path();
    // eckit::PathName Fam  = path / "fam";
    // return path.isDir() && Fam.exists();
    return eckit::FamRegionName(uri).exists();
}

static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing, const Config& config) {
    const Schema& schema = config.schema();
    schema.matchFirstLevel(key, keys, missing);
}

static void matchRequestToDB(const metkit::mars::MarsRequest& rq,
                             std::set<Key>&                   keys,
                             const char*                      missing,
                             const Config&                    config) {
    const Schema& schema = config.schema();
    schema.matchFirstLevel(rq, keys, missing);
}

static constexpr const char* regexForMissingValues = "[^:/]*";

std::set<eckit::PathName> FamEngine::databases(const std::set<Key>& /* keys */,
                                               const std::vector<eckit::PathName>& /* roots */,
                                               const Config& /* config */) const {
    NOTIMP;
}

std::vector<eckit::URI> FamEngine::databases(const Key& /* key */,
                                             const std::vector<eckit::PathName>& /* roots */,
                                             const Config& /* config */) const {
    NOTIMP;
}

std::vector<eckit::URI> FamEngine::databases(const metkit::mars::MarsRequest& /* request */,
                                             const std::vector<eckit::PathName>& /* roots */,
                                             const Config& /* config */) const {
    NOTIMP;
}

std::vector<eckit::URI> FamEngine::allLocations(const Key& key, const Config& config) const {
    NOTIMP;
    // return databases(key, CatalogueRootManager(config).allRoots(key), config);
}

std::vector<eckit::URI> FamEngine::visitableLocations(const Key& key, const Config& config) const {
    NOTIMP;
    // return databases(key, CatalogueRootManager(config).visitableRoots(key), config);
}

std::vector<URI> FamEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const {
    NOTIMP;
    // return databases(request, CatalogueRootManager(config).visitableRoots(request), config);
}

std::vector<eckit::URI> FamEngine::writableLocations(const Key& key, const Config& config) const {
    NOTIMP;
    // return databases(key, CatalogueRootManager(config).canArchiveRoots(key), config);
}

void FamEngine::print(std::ostream& out) const {
    out << "FamEngine()";
}

static EngineBuilder<FamEngine> builder;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
