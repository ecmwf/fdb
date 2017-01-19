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

#include "eckit/utils/Regex.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"

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

    eckit::PathName path("~/etc/fdb/engines");
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

const std::string& Manager::engine(const Key& key)
{
    pthread_once(&once, readEngineTypes);

    std::string expanded(key.valuesToString());

    /// @note returns the first engine that matches

    for (EngineTable::const_iterator i = engineTypes.begin(); i != engineTypes.end() ; ++i) {
        if(i->match(expanded)) {
            return i->engine();
        }
    }

    std::ostringstream oss;
    oss << "No FDB Engine type found for " << key << " (" << expanded << ")";
    throw eckit::SeriousBug(oss.str());
}

const std::string& Manager::engine(const PathName& path)
{
    std::vector<Engine*> engines = Engine::list();

    for(std::vector<Engine*>::const_iterator i = engines.begin(); i != engines.end(); ++i) {
        ASSERT(*i);
        const Engine& e = **i;
        if(e.canHandle(path)) {
            return e.dbType();
        }
    }
}

eckit::PathName Manager::location(const Key& key) {

    const std::string& name = Manager::engine(key);

    return Engine::backend(name).location(key);
}

std::vector<PathName> Manager::allLocations(const Key& key)
{
    /// @todo this needs to be a set_union for all the engines that match the key
    ///
    const std::string& name = Manager::engine(key);

    return Engine::backend(name).allLocations(key);
}


std::vector<eckit::PathName> Manager::visitableLocations(const Key& key) {

    /// @todo this needs to be a set_union for all the engines that match the key
    ///
    const std::string& name = Manager::engine(key);

    return Engine::backend(name).visitableLocations(key);
}

std::vector<eckit::PathName> Manager::writableLocations(const Key& key) {

    /// @todo this needs to be a set_union for all the engines that match the key
    ///
    const std::string& name = Manager::engine(key);

    return Engine::backend(name).writableLocations(key);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
