/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>

#include "eckit/utils/Regex.h"

#include "fdb5/LibFdb.h"

#include "fdb5/pmem/PMemEngine.h"
#include "fdb5/pmem/PoolManager.h"

using eckit::Regex;
using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string PMemEngine::name() const {
    return PMemEngine::typeName();
}

std::string PMemEngine::dbType() const {
    return PMemEngine::typeName();
}

eckit::PathName PMemEngine::location(const Key& key) const
{
    return PoolManager::pool(key);
}

static bool isPMemDB(const eckit::PathName& path) {
    eckit::PathName master = path / "master";
    return path.isDir() && master.exists();
}


bool PMemEngine::canHandle(const eckit::PathName& path) const
{
    return isPMemDB(path);
}


static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing)
{
    const Schema& schema = LibFdb::instance().schema();
    schema.matchFirstLevel(key, keys, missing);
}

std::vector<eckit::PathName> PMemEngine::databases(const Key &key, const std::vector<eckit::PathName>& dirs) {

    std::set<Key> keys;

    const char* regexForMissingValues = "[^:/]*";

    matchKeyToDB(key, keys, regexForMissingValues);

    Log::debug<LibFdb>() << "Matched DB keys " << keys << std::endl;

    std::vector<eckit::PathName> result;
    std::set<eckit::PathName> seen;

    for (std::vector<eckit::PathName>::const_iterator j = dirs.begin(); j != dirs.end(); ++j) {

        Log::debug<LibFdb>() << "Trying dir " << *j << std::endl;

        std::vector<eckit::PathName> subdirs;
        eckit::PathName::match((*j) / "*:*", subdirs, false);

        Log::debug<LibFdb>() << "Subdirs " << subdirs << std::endl;

        for (std::set<Key>::const_iterator i = keys.begin(); i != keys.end(); ++i) {

            Regex re("^" + (*i).valuesToString() + "$");

            for (std::vector<eckit::PathName>::const_iterator k = subdirs.begin(); k != subdirs.end(); ++k) {

                if(seen.find(*k) != seen.end()) {
                    continue;
                }

                if (re.match((*k).baseName())) {
                    try {
                        if(isPMemDB(*k)) {
                            result.push_back(*k);
                        }
                    } catch (eckit::Exception& e) {
                        Log::error() <<  "Error loading FDB database from " << *k << std::endl;
                        Log::error() << e.what() << std::endl;
                    }
                    seen.insert(*k);;
                }

            }
        }
    }

    return result;
}

std::vector<eckit::PathName> PMemEngine::allLocations(const Key& key) const
{
    return databases(key, PoolManager::allPools(key));
}

std::vector<eckit::PathName> PMemEngine::visitableLocations(const Key& key) const
{
    return databases(key, PoolManager::visitablePools(key));
}

std::vector<eckit::PathName> PMemEngine::writableLocations(const Key& key) const
{
    return databases(key, PoolManager::writablePools(key));
}

void PMemEngine::print(std::ostream& out) const
{
    out << "PMemEngine()";
}

static EngineBuilder<PMemEngine> pmem_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
