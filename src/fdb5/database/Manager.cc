/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/database/Manager.h"

#include "eckit/log/Log.h"
#include "eckit/utils/Regex.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Engine.h"

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
static pthread_once_t once = PTHREAD_ONCE_INIT;
static EngineTable engineTypes;


static void readEngineTypes() {

    eckit::PathName path("~fdb/etc/fdb/engines");
    std::ifstream in(path.localPath());

    eckit::Log::debug() << "Loading FDB engines from " << path << std::endl;

    if (!in) {
        eckit::Log::error() << path << eckit::Log::syserr << std::endl;
        return;
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

                engineTypes.push_back(EngineType(engine, regex));
                break;
            }

        default:
            eckit::Log::warning() << "FDB Manager: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

std::string Manager::engine(const Key& key)
{
    pthread_once(&once, readEngineTypes);

    std::string expanded(key.valuesToString());

    /// @note returns the first engine that matches

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

std::vector<std::string> Manager::engines(const Key& key)
{
    pthread_once(&once, readEngineTypes);

    std::string expanded(key.valuesToString());

    std::vector<std::string> r;

    for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end() ; ++i) {
        if(i->match(expanded)) {
            r.push_back(i->engine());
        }
    }

    if(r.empty()) {
        std::ostringstream oss;
        oss << "No FDB Engine type found for " << key << " (" << expanded << ")";
        throw eckit::SeriousBug(oss.str());
    }

    return r;
}

std::string Manager::engine(const PathName& path)
{
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

    return Engine::backend(name).location(key);
}

std::vector<PathName> Manager::allLocations(const Key& key)
{
    std::vector<std::string> engines = Manager::engines(key);

    Log::debug<LibFdb>() << "Matching engines for key " << key << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::vector<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).allLocations(key);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;
}


std::vector<eckit::PathName> Manager::visitableLocations(const Key& key) {

    std::vector<std::string> engines = Manager::engines(key);

    Log::debug<LibFdb>() << "Matching engines for key " << key << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::vector<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).visitableLocations(key);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;

}

std::vector<eckit::PathName> Manager::writableLocations(const Key& key) {

    std::vector<std::string> engines = Manager::engines(key);

    Log::debug<LibFdb>() << "Matching engines for key " << key << " -> " << engines << std::endl;

    std::vector<PathName> r; // union of all locations

    for(std::vector<std::string>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        Log::debug<LibFdb>() << "Selected FDB engine " << *i << std::endl;
        std::vector<PathName> p = Engine::backend(*i).writableLocations(key);
        r.insert(r.end(), p.begin(), p.end());
    }

    return r;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
