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

#include "eckit/log/Log.h"
#include "eckit/utils/Regex.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"
#include "eckit/thread/Mutex.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Engine.h"
#include "fdb5/rules/Schema.h"


using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct EngineType {

    EngineType(const std::string& name, const std::string& re) :
        name_(name),
        re_(re) {
    }

    const std::string& engine() const { return name_; }

    bool match(const std::string& s) const {  return re_.match(s); }

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

    if(!enginesFile.exists()) {
        eckit::Log::debug<LibFdb>() << "FDB Engines file not found: assuming Engine 'toc' Regex '.*'" << std::endl;
        table.push_back(EngineType("toc", ".*"));
        return table;
    }

    // Parse engines file

    std::ifstream in(enginesFile.localPath());

    eckit::Log::debug<LibFdb>() << "Loading FDB engines from " << enginesFile << std::endl;

    if (!in) {
        eckit::Log::error() << enginesFile << eckit::Log::syserr << std::endl;
        return table;
    }

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
            case 2: {
                const std::string& regex  = s[0];
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

Manager::Manager(const Config& config) :
    config_(config) {

    static std::string baseEnginesFile = eckit::Resource<std::string>("fdbEnginesFile;$FDB_ENGINES_FILE", "~fdb/etc/fdb/engines");

    enginesFile_ = config.expandPath(baseEnginesFile);

    explicitEngine_ = config.getString("engine", "");
}

Manager::~Manager() {}


std::string Manager::engine(const Key& key)
{
    // If we have set the engine in the config, use that
    if (!explicitEngine_.empty()) return explicitEngine_;

    std::string expanded(key.valuesToString());

    /// @note returns the first engine that matches

    const EngineTable& engineTypes(readEngineTypes(enginesFile_));

    for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end() ; ++i) {
        if(i->match(expanded)) {
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

std::set<std::string> Manager::engines(const Key& key)
{
    std::set<std::string> s;
    std::string expanded;

    if (!explicitEngine_.empty()) {
        expanded = explicitEngine_;
        s.insert(explicitEngine_);
    } else {

        // Determine matching engine types from the engines file

        expanded = key.valuesToString();
        const EngineTable& engineTypes(readEngineTypes(enginesFile_));

        for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end() ; ++i) {
            if(key.empty() || i->match(expanded)) {
                s.insert(i->engine());
            }
        }
    }

    if(s.empty()) {
        std::ostringstream oss;
        oss << "No FDB Engine type found for " << key << " (" << expanded << ")";
        throw eckit::SeriousBug(oss.str());
    }

    return s;
}

std::set<std::string> Manager::engines(const metkit::MarsRequest& rq)
{
    std::set<std::string> s;
    std::string expanded;

    if (!explicitEngine_.empty()) {
        expanded = explicitEngine_;
        s.insert(explicitEngine_);
    } else {

        Key key;
        if (!config_.schema().expandFirstLevel(rq, key)) {
            std::stringstream ss;
            ss << "Could not uniquely expand first level key of request: " << rq << std::endl;
            throw eckit::SeriousBug(ss.str(), Here());
        }

        expanded = key.valuesToString();
        const EngineTable& engineTypes(readEngineTypes(enginesFile_));

        for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end() ; ++i) {
            if(key.empty() || i->match(expanded)) {
                s.insert(i->engine());
            }
        }
    }

    if(s.empty()) {
        std::ostringstream oss;
        oss << "No FDB Engine type found for " << rq << " (" << expanded << ")";
        throw eckit::SeriousBug(oss.str(), Here());
    }

    return s;
}

std::string Manager::engine(const PathName& path)
{
    // If we have set the engine in the config, use that
    if (!explicitEngine_.empty()) return explicitEngine_;

    // Otherwise, check which engines can handle the given path.

    std::vector<Engine*> engines = EngineRegistry::engines();

    for(std::vector<Engine*>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        ASSERT(*i);
        const Engine& e = **i;
        if(e.canHandle(path)) {
            return e.dbType();
        }
    }

    std::ostringstream oss;
    oss << "No FDB engine can recognise path " << path;
    throw eckit::BadParameter(oss.str(), Here());
}

eckit::PathName Manager::location(const Key& key) {

    const std::string& name = Manager::engine(key);

    return Engine::backend(name).location(key, config_);
}

std::vector<PathName> Manager::allLocations(const Key& key)
{
    std::set<std::string> engines = Manager::engines(key);

    Log::debug<LibFdb>() << "Matching engines for key " << key << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::set<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).allLocations(key, config_);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;
}


std::vector<eckit::PathName> Manager::visitableLocations(const metkit::MarsRequest& rq) {

    std::set<std::string> engines = Manager::engines(rq);

    Log::debug<LibFdb>() << "Matching engines for request " << rq << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::set<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).visitableLocations(rq, config_);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;

}

std::vector<eckit::PathName> Manager::writableLocations(const Key& key) {

    std::set<std::string> engines = Manager::engines(key);

    Log::debug<LibFdb>() << "Matching engines for key " << key << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::set<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).writableLocations(key, config_);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
