/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/pmem/PoolManager.h"

#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Key.h"
#include "fdb5/pmem/PoolEntry.h"
#include "fdb5/pmem/PoolGroup.h"

#include <mutex>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

typedef std::vector<fdb5::PoolGroup> PoolGroupTable;
typedef std::map<eckit::PathName, PoolGroupTable> PoolGroupMap;

std::mutex poolGroupMutex;
static PoolGroupMap poolGroupTables;


static std::vector<PoolEntry> readPools(const eckit::PathName& path) {

    std::vector<PoolEntry> result;

    std::ifstream in(path.localPath());

    eckit::Log::debug() << "Loading FDB pools from " << path << std::endl;

    if (!in) {
        eckit::Log::error() << path << eckit::Log::syserr << std::endl;
        return result;
    }

    eckit::Translator<std::string,bool> str2bool;

    eckit::Tokenizer parse(" ");

    char line[1024];
    while (in.getline(line, sizeof(line))) {

        std::vector<std::string> s;
        parse(line, s);

        size_t i = 0;
        while (i < s.size()) {
            if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            } else {
                i++;
            }
        }

        if (s.size() == 0 || s[0][0] == '#') {
            continue;
        }

        switch (s.size()) {
            case 4: {
                const std::string& path       = s[0];
                const std::string& poolgroup  = s[1];
                bool writable        = str2bool(s[2]);
                bool visit           = str2bool(s[3]);

                result.push_back(PoolEntry(path, poolgroup, writable, visit));
                break;
            }

        default:
            eckit::Log::warning() << "FDB PoolManager: Invalid line ignored: " << line << std::endl;
            break;
        }
    }

    return result;
}

static std::vector<PoolEntry> poolsInGroup(const std::vector<PoolEntry>& all, const std::string& poolgroup) {

    std::vector<PoolEntry> pools;

    for (std::vector<PoolEntry>::const_iterator i = all.begin(); i != all.end() ; ++i) {
        if (i->poolgroup() == poolgroup) {
            pools.push_back(*i);
        }
    }
    return pools;
}


static PoolGroupTable readPoolGroups(const eckit::PathName& fdbHome) {

    std::lock_guard<std::mutex> lock(poolGroupMutex);

    // Pool groups are memoised, so only read it once

    PoolGroupMap::const_iterator it = poolGroupTables.find(fdbHome);
    if (it != poolGroupTables.end()) {
        return it->second;
    }

    eckit::PathName fdbPMemPoolsFile = eckit::Resource<eckit::PathName>("fdbPMemPoolsFile;$FDB_PMEM_POOLS_FILE", fdbHome / "pools");
    std::vector<PoolEntry> allPools = readPools(fdbPMemPoolsFile);

    eckit::PathName fdbPMemPoolGroupsFile = eckit::Resource<eckit::PathName>("fdbPMemPoolGroupsFile;$FDB_PMEM_POOLGROUPS_FILE", fdbHome / "poolgroups");
    std::ifstream in(fdbPMemPoolGroupsFile.localPath());

    eckit::Log::debug() << "Loading FDB file poolgroups from " << fdbPMemPoolGroupsFile << std::endl;

    if (!in) {
        throw eckit::ReadError(fdbPMemPoolGroupsFile, Here());
    }

    PoolGroupTable& poolgroupsTable(poolGroupTables[fdbHome]);

    eckit::Tokenizer parse(" ");

    char line[1024];
    while (in.getline(line, sizeof(line))) {

        std::vector<std::string> s;
        parse(line, s);

        size_t i = 0;
        while (i < s.size()) {
            if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            } else {
                i++;
            }
        }

        if (s.size() == 0 || s[0][0] == '#') {
            continue;
        }

        switch (s.size()) {
            case 3: {
                const std::string& regex     = s[0];
                const std::string& poolgroup = s[1];
                const std::string& handler   = s[2];

                std::vector<PoolEntry> pools = poolsInGroup(allPools, poolgroup);

                if(!pools.size()) {
                    std::ostringstream oss;
                    oss << "No pools found for poolgroup " << poolgroup;
                    throw UserError(oss.str(), Here());
                }

                poolgroupsTable.push_back(PoolGroup(poolgroup, regex, handler, pools));
                break;
            }

        default:
            eckit::Log::warning() << "FDB PoolManager: Invalid line ignored: " << line << std::endl;
            break;

        }
    }

    return poolgroupsTable;
}

static PoolGroupTable poolGroups(const Config& config) {

    static std::string poolGroupFile = eckit::Resource<std::string>("fdbPMemPoolsFile;$FDB_PMEM_POOLS_FILE", "~fdb/etc/fdb/pools");

    if (config.has("groups")) {
        PoolGroupTable table;
        std::vector<LocalConfiguration> groupsConfigs(config.getSubConfigurations("groups"));
        for (const auto& group : groupsConfigs) {

            std::vector<PoolEntry> poolEntries;
            std::vector<LocalConfiguration> pools(group.getSubConfigurations("pools"));
            for (const auto& pool : pools) {
                poolEntries.emplace_back(
                    PoolEntry(
                        pool.getString("path"),
                        "",
                        pool.getBool("writable", true),
                        pool.getBool("visit", true)
                    )
                );
            }

            table.emplace_back(
                PoolGroup(
                    group.getString("name", ""),
                    group.getString("regex", ".*"),
                    group.getString("handler", "Default"),
                    poolEntries
                )
            );
        }
        return table;
    } else {
        return readPoolGroups(config.expandPath("~fdb/"));
    }
}


//----------------------------------------------------------------------------------------------------------------------

PoolManager::PoolManager(const Config& config) :
    poolGroupTable_(poolGroups(config)) {}


eckit::PathName PoolManager::pool(const Key& key) {

    std::string name(key.valuesToString());

    /// @note returns the first PoolGroup that matches

    for (const PoolGroup& group : poolGroupTable_) {
        if(group.match(name)) {
            PathName path = group.pool(key);
            return path / name;
        }
    }

    std::ostringstream oss;
    oss << "No FDB memory pool for " << key << " (" << name << ")";
    throw eckit::SeriousBug(oss.str());
}

std::vector<PathName> PoolManager::allPools(const Key& key)
{
    eckit::StringSet pools;

    std::string k = key.valuesToString();

    for (const PoolGroup& group : poolGroupTable_) {
        if(group.match(k)) {
            group.all(pools);
        }
    }

    return std::vector<eckit::PathName>(pools.begin(), pools.end());
}


std::vector<eckit::PathName> PoolManager::visitablePools(const Key& key) {

    eckit::StringSet pools;

    std::string k = key.valuesToString();

    for (const PoolGroup& group : poolGroupTable_) {
        if(group.match(k)) {
            group.visitable(pools);
        }
    }

    Log::debug<LibFdb>() << "Visitable Pools " << pools << std::endl;

    return std::vector<eckit::PathName>(pools.begin(), pools.end());
}

std::vector<eckit::PathName> PoolManager::writablePools(const Key& key) {

    eckit::StringSet pools;

    std::string k = key.valuesToString();

    for (const PoolGroup& group : poolGroupTable_) {
        if(group.match(k)) {
            group.writable(pools);
        }
    }

    return std::vector<eckit::PathName>(pools.begin(), pools.end());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
