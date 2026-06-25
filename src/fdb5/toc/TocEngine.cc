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

#include <algorithm>
#include <cstring>
#include <list>
#include <ostream>
#include <regex>

#include "eckit/eckit.h"

#include "eckit/config/Resource.h"
#include "eckit/filesystem/LocalFileManager.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/StdDir.h"
#include "eckit/log/Log.h"
#include "eckit/os/BackTrace.h"
#include "eckit/os/Stat.h"
#include "eckit/utils/StringTools.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/types/TypesRegistry.h"

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void TocEngine::scan_dbs(const std::string& path, std::list<std::string>& dbs) const {

    eckit::StdDir d(path.c_str());

    // Once readdir_r finally gets deprecated and removed, we may need to
    // protecting readdir() as not yet guarranteed thread-safe by POSIX
    // technically it should only be needed on a per-directory basis
    // this should be a resursive mutex
    // AutoLock<Mutex> lock(mutex_);

    for (;;) {
        struct dirent* e = d.dirent();
        if (e == nullptr) {
            break;
        }

        if (e->d_name[0] == '.') {
            if (e->d_name[1] == 0 || (e->d_name[1] == '.' && e->d_name[2] == 0)) {
                continue;
            }
        }

        std::string full = path;
        if (path[path.length() - 1] != '/') {
            full += "/";
        }
        full += e->d_name;

        bool do_stat = true;

#if defined(eckit_HAVE_DIRENT_D_TYPE)
        do_stat = false;
        if (e->d_type == DT_DIR) {
            dbs.push_back(full);
        }
        else if (e->d_type == DT_UNKNOWN) {
            do_stat = true;
        }
#endif
        if (do_stat) {
            eckit::Stat::Struct info;
            if (eckit::Stat::stat(full.c_str(), &info) == 0) {
                if (S_ISDIR(info.st_mode)) {
                    dbs.push_back(full);
                }
            }
            else {
                Log::error() << "Cannot stat " << full << Log::syserr << std::endl;
            }
        }
    }
}

std::string TocEngine::name() const {
    return TocEngine::typeName();
}

std::string TocEngine::dbType() const {
    return TocEngine::typeName();
}

eckit::URI TocEngine::location(const Key& key, const Config& config) const {
    return URI("toc", CatalogueRootManager(config).directory(key).directory_);
}

bool TocEngine::canHandle(const eckit::URI& uri, const Config& config) const {
    if (uri.scheme() != "toc") {
        return false;
    }

    eckit::PathName path = uri.path();
    eckit::PathName toc = path / "toc";
    return path.isDir() && toc.exists();
}

static void matchKeyToDB(const Key& key, std::map<Key, const Rule*>& keys, const char* missing, const Config& config) {
    const Schema& schema = config.schema();
    schema.matchDatabase(key, keys, missing);
}

static void matchRequestToDB(const metkit::mars::MarsRequest& rq, std::map<Key, const Rule*>& keys, const char* missing,
                             const Config& config) {
    const Schema& schema = config.schema();
    schema.matchDatabase(rq, keys, missing);
}

static constexpr const char* regexForMissingValues = "[^:/]*";

std::map<eckit::PathName, const Rule*> TocEngine::databases(const std::map<Key, const Rule*>& keys,
                                                            const std::vector<eckit::PathName>& roots,
                                                            const Config& config) const {

    std::map<eckit::PathName, const Rule*> result;

    for (std::vector<eckit::PathName>::const_iterator j = roots.begin(); j != roots.end(); ++j) {

        LOG_DEBUG_LIB(LibFdb5) << "Scanning for TOC FDBs in root " << *j << std::endl;

        std::string regex_prefix = "^" + eckit::StringTools::lower(Regex::escape(j->asString()));

        if (regex_prefix.back() != '/') {
            regex_prefix += "/";
        }

        std::list<std::string> dbs;
        scan_dbs(*j, dbs);

        for (const auto& [key, rule] : keys) {

            std::vector<std::string> dbpaths =
                CatalogueRootManager(config).possibleDbPathNames(key, regexForMissingValues);

            for (const std::string& dbpath : dbpaths) {

                std::string regex = regex_prefix + eckit::StringTools::lower(dbpath) + "$";
                eckit::Regex reg(regex);

                LOG_DEBUG_LIB(LibFdb5) << " -> key " << key << " dbpath " << dbpath << " pathregex " << regex
                                       << std::endl;

                for (const auto& db : dbs) {

                    LOG_DEBUG_LIB(LibFdb5) << "    -> db " << db << std::endl;

                    if (result.find(db) != result.end()) {
                        continue;
                    }
                    if (reg.match(eckit::StringTools::lower(db))) {
                        result[db] = rule;
                    }
                }
            }
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "TocEngine::databases() results [";
    for (const auto& [path, rule] : result) {
        LOG_DEBUG_LIB(LibFdb5) << path << std::endl;
    }
    LOG_DEBUG_LIB(LibFdb5) << "]" << std::endl;

    return result;
}

std::vector<eckit::URI> TocEngine::databases(const Key& key, const std::vector<eckit::PathName>& roots,
                                             const Config& config) const {

    std::map<Key, const Rule*> keys;

    matchKeyToDB(key, keys, regexForMissingValues, config);

    LOG_DEBUG_LIB(LibFdb5) << "Matched DB schemas for key " << key << " -> keys " << keys << std::endl;

    std::map<eckit::PathName, const Rule*> databasesMatchRegex(databases(keys, roots, config));

    std::vector<eckit::URI> result;
    for (const auto& [path, rule] : databasesMatchRegex) {
        try {
            TocHandler toc(path, config);
            if (toc.databaseKey().match(key)) {
                LOG_DEBUG_LIB(LibFdb5) << " found match with " << path << std::endl;
                result.push_back(eckit::URI(TocEngine::typeName(), path));
            }
        }
        catch (eckit::Exception& e) {
            eckit::Log::error() << "Error loading FDB database from " << path << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }

    return result;
}

std::vector<eckit::URI> TocEngine::databases(const metkit::mars::MarsRequest& request,
                                             const std::vector<eckit::PathName>& roots, const Config& config) const {

    std::map<Key, const Rule*> keys;

    matchRequestToDB(request, keys, "", config);

    LOG_DEBUG_LIB(LibFdb5) << "Matched DB schemas for request " << request << " -> keys [";
    for (const auto& [key, rule] : keys) {
        LOG_DEBUG_LIB(LibFdb5) << key << " | " << *rule << ",";
    }
    LOG_DEBUG_LIB(LibFdb5) << "]" << std::endl;

    std::map<eckit::PathName, const Rule*> databasesMatchRegex(databases(keys, roots, config));

    std::vector<eckit::URI> result;
    for (const auto& [p, rule] : databasesMatchRegex) {
        try {
            /// @todo we don't have to open tocs to check if they match the request
            if (p.exists()) {
                eckit::PathName path = p.isDir() ? p : p.dirName();
                path = path.realName();

                LOG_DEBUG_LIB(LibFdb5) << "FDB processing Path " << path << std::endl;

                TocHandler toc(path, config);
                auto canonical = rule->registry().canonicalise(request);
                if (toc.databaseKey().partialMatch(canonical)) {
                    LOG_DEBUG_LIB(LibFdb5) << " found match with " << path << std::endl;
                    result.push_back(eckit::URI(TocEngine::typeName(), path));
                }
            }
        }
        catch (eckit::Exception& e) {
            eckit::Log::error() << "Error loading FDB database from " << p << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }

    return result;
}

std::vector<eckit::URI> TocEngine::visitableLocations(const Key& key, const Config& config) const {
    return databases(key, CatalogueRootManager(config).visitableRoots(key), config);
}

std::vector<URI> TocEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const {
    return databases(request, CatalogueRootManager(config).visitableRoots(request), config);
}

void TocEngine::print(std::ostream& out) const {
    out << "TocEngine()";
}

static EngineBuilder<TocEngine> toc_builder;

static eckit::LocalFileManager manager_toc("toc");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
