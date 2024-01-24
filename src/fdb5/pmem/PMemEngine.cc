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
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <ostream>

#include "eckit/utils/Regex.h"
#include "eckit/io/AutoCloser.h"

#include "fdb5/LibFdb5.h"

#include "fdb5/pmem/PMemEngine.h"
#include "fdb5/pmem/PoolManager.h"
#include "fdb5/pmem/PMemDBReader.h"
#include "fdb5/rules/Schema.h"

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

eckit::PathName PMemEngine::location(const Key& key, const Config& config) const
{
    return PoolManager(config).pool(key);
}

static bool isPMemDB(const eckit::PathName& path) {
    eckit::PathName master = path / "master";
    return path.isDir() && master.exists();
}


bool PMemEngine::canHandle(const eckit::PathName& path) const
{
    return isPMemDB(path);
}


static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing, const Config& config)
{
    const Schema& schema = config.schema();
    schema.matchFirstLevel(key, keys, missing);
}

static void matchRequestToDB(const metkit::mars::MarsRequest& rq, std::set<Key>& keys, const char* missing, const Config& config)
{
    const Schema& schema = config.schema();
    schema.matchFirstLevel(rq, keys, missing);
}

std::vector<eckit::PathName> PMemEngine::databases(const Key &key,
                                                   const std::vector<eckit::PathName>& dirs,
                                                   const Config& config) {

    std::set<Key> keys;

    const char* regexForMissingValues = "[^:/]*";

    matchKeyToDB(key, keys, regexForMissingValues, config);

    LOG_DEBUG_LIB(LibFdb5) << "Matched DB keys " << keys << std::endl;

    return databases(keys, dirs, config);
}

std::vector<eckit::PathName> PMemEngine::databases(const metkit::mars::MarsRequest& request,
                                                   const std::vector<eckit::PathName>& dirs,
                                                   const Config& config) {

    std::set<Key> keys;

    const char* regexForMissingValues = "[^:/]*";

    matchRequestToDB(request, keys, regexForMissingValues, config);

    LOG_DEBUG_LIB(LibFdb5) << "Matched DB keys " << keys << std::endl;

    std::vector<eckit::PathName> result;
    for (const auto& path : databases(keys, dirs, config)) {
        try {
            pmem::PMemDBReader db(path, config);
            if (db.key().partialMatch(request)) {
                result.push_back(path);
            }
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading PMem FDB database from " << path << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }
    }
    return result;
}

std::vector<eckit::PathName> PMemEngine::databases(const std::set<Key>& keys,
                                                   const std::vector<eckit::PathName>& dirs,
                                                   const Config& config) {

    std::vector<eckit::PathName> result;
    std::set<eckit::PathName> seen;

    for (std::vector<eckit::PathName>::const_iterator j = dirs.begin(); j != dirs.end(); ++j) {

        LOG_DEBUG_LIB(LibFdb5) << "Trying dir " << *j << std::endl;

        std::vector<eckit::PathName> subdirs;
        eckit::PathName::match((*j) / "*:*", subdirs, false);

        LOG_DEBUG_LIB(LibFdb5) << "Subdirs " << subdirs << std::endl;

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

std::vector<eckit::PathName> PMemEngine::allLocations(const Key& key, const Config& config) const
{
    return databases(key, PoolManager(config).allPools(key), config);
}

std::vector<eckit::PathName> PMemEngine::visitableLocations(const Key& key, const Config& config) const
{
    return databases(key, PoolManager(config).visitablePools(key), config);
}

std::vector<eckit::PathName> PMemEngine::visitableLocations(const metkit::mars::MarsRequest& rq, const Config& config) const
{
    return databases(rq, PoolManager(config).visitablePools(rq), config);
}

std::vector<eckit::PathName> PMemEngine::writableLocations(const Key& key, const Config& config) const
{
    return databases(key, PoolManager(config).writablePools(key), config);
}

void PMemEngine::print(std::ostream& out) const
{
    out << "PMemEngine()";
}

static EngineBuilder<PMemEngine> pmem_builder;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
