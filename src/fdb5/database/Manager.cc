/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/database/Manager.h"

#include <fstream>

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/utils/Literals.h"
#include "eckit/utils/Regex.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Engine.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"


using namespace eckit;
using namespace eckit::literals;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct EngineType {

    EngineType(const std::string& name, const std::string& re) : name_(name), re_(re) {}

    const std::string& engine() const { return name_; }

    bool match(const std::string& s) const { return re_.match(s); }

    std::string name_;
    eckit::Regex re_;
};

//----------------------------------------------------------------------------------------------------------------------

typedef std::vector<EngineType> EngineTable;
typedef std::map<eckit::PathName, EngineTable> EngineTableMap;

/// We can have multiple engine configurations, so track them.
static eckit::Mutex engineTypesMutex;
static EngineTableMap engineTypes;


static const EngineTable& readEngineTypes(const eckit::PathName enginesFile) {

    eckit::AutoLock<eckit::Mutex> lock(engineTypesMutex);

    // Table is memoised, so we only read it once. Check if we have it.

    EngineTableMap::const_iterator it = engineTypes.find(enginesFile);
    if (it != engineTypes.end()) {
        return it->second;
    }

    EngineTable& table(engineTypes[enginesFile]);

    // Sensible defaults if not configured

    if (!enginesFile.exists()) {
        LOG_DEBUG_LIB(LibFdb5) << "FDB Engines file not found: assuming Engine 'toc' Regex '.*'" << std::endl;
        table.push_back(EngineType("toc", ".*"));
        return table;
    }

    // Parse engines file

    std::ifstream in(enginesFile.localPath());

    LOG_DEBUG_LIB(LibFdb5) << "Loading FDB engines from " << enginesFile << std::endl;

    if (!in) {
        eckit::Log::error() << enginesFile << eckit::Log::syserr << std::endl;
        return table;
    }

    eckit::Tokenizer parse(" ");

    char line[1_KiB];
    while (in.getline(line, sizeof(line))) {

        std::vector<std::string> s;
        parse(line, s);

        size_t i = 0;
        while (i < s.size()) {
            if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            }
            else {
                i++;
            }
        }

        if (s.size() == 0 || s[0][0] == '#') {
            continue;
        }

        switch (s.size()) {
            case 2: {
                const std::string& regex = s[0];
                const std::string& engine = s[1];

                table.push_back(EngineType(engine, regex));
                break;
            }

            default:
                eckit::Log::warning() << "FDB Manager: Invalid line ignored: " << line << std::endl;
                break;
        }
    }

    return table;
}

//----------------------------------------------------------------------------------------------------------------------

Manager::Manager(const Config& config) : config_(config) {

    static std::string baseEnginesFile =
        eckit::Resource<std::string>("fdbEnginesFile;$FDB_ENGINES_FILE", "~fdb/etc/fdb/engines");

    enginesFile_ = config.expandPath(baseEnginesFile);

    explicitEngine_ = config.getString("engine", "");
}

Manager::~Manager() {}


std::string Manager::engine(const Key& key) {
    // If we have set the engine in the config, use that
    if (!explicitEngine_.empty()) {
        return explicitEngine_;
    }

    std::string expanded(key.valuesToString());

    /// @note returns the first engine that matches

    const EngineTable& engineTypes(readEngineTypes(enginesFile_));

    for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end(); ++i) {
        if (i->match(expanded)) {
            const std::string& engine = i->engine();
            if (EngineRegistry::has(engine)) {
                return engine;
            }
            else {
                Log::warning() << "Configured FDB engine " << engine << " is not registered" << std::endl;
            }
        }
    }

    std::ostringstream oss;
    oss << "No FDB Engine type found for " << key << " (" << expanded << ")";
    Log::error() << oss.str() << std::endl;
    throw eckit::SeriousBug(oss.str());
}

std::set<std::string> Manager::engines(const Key& key) {
    std::set<std::string> s;
    std::string expanded;

    if (!explicitEngine_.empty()) {
        expanded = explicitEngine_;
        s.insert(explicitEngine_);
    }
    else {

        // Determine matching engine types from the engines file

        expanded = key.valuesToString();
        const EngineTable& engineTypes(readEngineTypes(enginesFile_));

        for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end(); ++i) {
            if (key.empty() || i->match(expanded)) {
                s.insert(i->engine());
            }
        }
    }

    if (s.empty()) {
        std::ostringstream oss;
        oss << "No FDB Engine type found for " << key << " (" << expanded << ")";
        throw eckit::SeriousBug(oss.str());
    }

    return s;
}

std::set<std::string> Manager::engines(const metkit::mars::MarsRequest& rq, bool all) {
    std::set<std::string> s;
    std::string expanded;

    if (!explicitEngine_.empty()) {
        expanded = explicitEngine_;
        s.insert(explicitEngine_);
    }
    else {

        const EngineTable& engineTypes(readEngineTypes(enginesFile_));

        if (all) {
            for (auto e = engineTypes.begin(); e != engineTypes.end(); ++e) {
                s.insert(e->engine());
            }
        }
        else {

            // Match all possible expansions of the first level according to the schema
            std::map<Key, const Rule*> keys;
            config_.schema().matchDatabase(rq, keys, "");

            std::set<std::string> expandedKeys;
            for (const auto& [k, r] : keys) {
                expandedKeys.insert(k.valuesToString());
            }

            for (auto e = engineTypes.begin(); e != engineTypes.end(); ++e) {
                for (auto expanded = expandedKeys.begin(); expanded != expandedKeys.end(); ++expanded) {
                    if (e->match(*expanded)) {
                        s.insert(e->engine());
                    }
                }
            }
        }
    }

    if (s.empty()) {
        std::ostringstream oss;
        oss << "No FDB Engine type found for " << rq << " (" << expanded << ")";
        throw eckit::SeriousBug(oss.str(), Here());
    }

    return s;
}

std::string Manager::engine(const URI& uri) {
    // If we have set the engine in the config, use that
    if (!explicitEngine_.empty()) {
        return explicitEngine_;
    }

    // Otherwise, check which engines can handle the given path.

    std::vector<Engine*> engines = EngineRegistry::engines();

    for (std::vector<Engine*>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        ASSERT(*i);
        const Engine& e = **i;
        if (e.canHandle(uri, config_)) {
            return e.dbType();
        }
    }

    std::ostringstream oss;
    oss << "No FDB engine can recognise path " << uri.asRawString();
    throw eckit::BadParameter(oss.str(), Here());
}

std::vector<eckit::URI> Manager::visitableLocations(const metkit::mars::MarsRequest& rq, bool all) {

    std::set<std::string> engines = Manager::engines(rq, all);

    LOG_DEBUG_LIB(LibFdb5) << "Matching engines for request " << rq << (all ? " ALL" : "") << " -> " << engines
                           << std::endl;

    std::vector<URI> r;  // union of all locations

    for (std::set<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        LOG_DEBUG_LIB(LibFdb5) << "Selected FDB engine " << *i << std::endl;
        std::vector<URI> p;
        if (all) {
            p = Engine::backend(*i).visitableLocations(Key(), config_);
        }
        else {
            p = Engine::backend(*i).visitableLocations(rq, config_);
        }

        r.insert(r.end(), p.begin(), p.end());
    }

    return r;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
