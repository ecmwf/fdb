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

#include "eckit/eckit_config.h"

#include "eckit/log/Log.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/utils/Regex.h"
#include "eckit/os/BackTrace.h"
#include "eckit/os/Stat.h"

#include "fdb5/LibFdb.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/toc/TocHandler.h"

using eckit::Regex;
using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class StdDir {
    DIR *d_;
public:
    StdDir(const char* d) { d_ = ::opendir(d);}
    ~StdDir()             { if(d_) ::closedir(d_); }
    operator DIR*()       { return d_; }
};

static void scan_dbs(const std::string& path, std::list<std::string>& dbs)
{
    StdDir d(path.c_str());

    if(d == 0)
    {
        Log::error() << "opendir(" << path << ")" << Log::syserr << std::endl;
        throw FailedSystemCall("opendir");
    }

    struct dirent buf;

    for(;;)
    {
        struct dirent *e;
#ifdef ECKIT_HAVE_READDIR_R
        errno = 0;
        if(::readdir_r(d,&buf,&e) != 0)
        {
            if(errno)
                throw FailedSystemCall("readdir_r");
            else
                e = 0;
        }
#else
        e = ::readdir(d);
#endif

        if(e == 0)
            break;

        if(e->d_name[0] == '.')
            if(e->d_name[1] == 0 || (e->d_name[1] =='.' && e->d_name[2] == 0))
                continue;

        if(::strcmp(e->d_name, "toc") == 0) {
            dbs.push_back(path);
        }

        std::string full = path + "/" + e->d_name;

#if defined(ECKIT_HAVE_DIRENT_D_TYPE) || defined(DT_DIR)
        if(e->d_type == DT_DIR) {
            scan_dbs(full.c_str(), dbs);
        }
#else
        eckit::Stat::Struct info;
        if(eckit::Stat::stat(full.c_str(), &info) == 0)
        {
            if(S_ISDIR(info.st_mode))
                scan_dbs(full.c_str(), dbs);
        }
        else Log::error() << "Cannot stat " << full << Log::syserr << std::endl;
#endif
    }
}

//----------------------------------------------------------------------------------------------------------------------

std::string TocEngine::name() const {
    return TocEngine::typeName();
}

std::string TocEngine::dbType() const {
    return TocEngine::typeName();
}

eckit::PathName TocEngine::location(const Key& key, const Config& config) const
{
    return RootManager(config).directory(key);
}

bool TocEngine::canHandle(const eckit::PathName& path) const
{
    eckit::PathName toc = path / "toc";
    return path.isDir() && toc.exists();
}

static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing, const Config& config)
{
    const Schema& schema = config.schema();
    schema.matchFirstLevel(key, keys, missing);
}

std::vector<eckit::PathName> TocEngine::databases(const Key& key,
                                                  const std::vector<eckit::PathName>& roots,
                                                  const Config& config) {

    std::set<Key> keys;

    const char* regexForMissingValues = "[^:/]*";

    matchKeyToDB(key, keys, regexForMissingValues, config);

    Log::debug<LibFdb>() << "Matched DB schemas for key " << key << " -> keys " << keys << std::endl;

    std::vector<eckit::PathName> result;
    std::set<eckit::PathName> seen;

    for (std::vector<eckit::PathName>::const_iterator j = roots.begin(); j != roots.end(); ++j) {

        Log::debug<LibFdb>() << "Scanning for TOC FDBs in root " << *j << std::endl;

        std::list<std::string> dbs;
        scan_dbs(*j, dbs);

        for (std::set<Key>::const_iterator i = keys.begin(); i != keys.end(); ++i) {

            std::vector<std::string> dbpaths = RootManager(config).possibleDbPathNames(*i, regexForMissingValues);

            for(std::vector<std::string>::const_iterator dbpath = dbpaths.begin(); dbpath != dbpaths.end(); ++dbpath) {

                Regex re("^" + *j + "/" + *dbpath + "$");

                Log::debug<LibFdb>() << " -> key i " << *i
                                     << " dbpath " << *dbpath
                                     << " pathregex " << re << std::endl;

                for (std::list<std::string>::const_iterator k = dbs.begin(); k != dbs.end(); ++k) {

                    Log::debug<LibFdb>() << "    -> db " << *k << std::endl;

                    if(seen.find(*k) != seen.end()) {
                        continue;
                    }

                    if (re.match(*k)) {
                        try {
                            TocHandler toc(*k);
                            if (toc.databaseKey().match(key)) {
                                Log::debug<LibFdb>() << " found match with " << *k << std::endl;
                                result.push_back(*k);
                            }
                        } catch (eckit::Exception& e) {
                            eckit::Log::error() <<  "Error loading FDB database from " << *k << std::endl;
                            eckit::Log::error() << e.what() << std::endl;
                        }
                        seen.insert(*k);
                    }
                }
            }
        }
    }

    Log::debug<LibFdb>() << "TocEngine::databases() results " << result << std::endl;

    return result;
}


std::vector<eckit::PathName> TocEngine::allLocations(const Key& key, const Config& config) const
{
    return databases(key, RootManager(config).allRoots(key), config);
}

std::vector<eckit::PathName> TocEngine::visitableLocations(const Key& key, const Config& config) const
{
    return databases(key, RootManager(config).visitableRoots(key), config);
}

std::vector<eckit::PathName> TocEngine::writableLocations(const Key& key, const Config& config) const
{
    return databases(key, RootManager(config).writableRoots(key), config);
}

void TocEngine::print(std::ostream& out) const
{
    out << "TocEngine()";
}

static EngineBuilder<TocEngine> toc_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
