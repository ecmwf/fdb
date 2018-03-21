/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/utils/Translator.h"
#include "eckit/types/Types.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/FDBConfig.h"
#include "fdb5/dist/DistManager.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// TODO: Lanes should be JSON configurable, as well as just using home_

class FDBDistLane {

public: // methods

    FDBDistLane(const eckit::PathName& home, bool writable, bool visit);

    friend std::ostream& operator<<(std::ostream& s, const FDBDistLane& l) {
        l.print(s);
        return s;
    }

private: // methods

    void print(std::ostream& out) const;

private: // members

    /// The FDB_HOME for the delegated fdb5
    eckit::PathName home_;

    bool writable_;
    bool visit_;
};

} // namespace fdb5

// For ostream specialisation
namespace eckit {
    template <> struct VectorPrintSelector<fdb5::FDBDistLane> { typedef VectorPrintSimple selector; };
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBDistPool {

public: // methods

    FDBDistPool(const std::string& name, const std::string& re);

    const std::string& name() const;

    bool match(const std::string& s) const;

    void appendLane(const FDBDistLane& lane);

    const std::vector<FDBDistLane>& lanes() const;

    friend std::ostream& operator<<(std::ostream& s, const FDBDistPool& p) {
        p.print(s);
        return s;
    }

private: // methods

    void print(std::ostream& out) const;

private: // members

    std::string name_;

    eckit::Regex re_;

    std::vector<FDBDistLane> lanes_;
};


//----------------------------------------------------------------------------------------------------------------------


FDBDistPool::FDBDistPool(const std::string& name, const std::string& re) :
    name_(name),
    re_(re) {}


const std::string& FDBDistPool::name() const {
    return name_;
}


bool FDBDistPool::match(const std::string& s) const {
    return re_.match(s);
}


void FDBDistPool::appendLane(const FDBDistLane& lane) {
    lanes_.push_back(lane);
}


const std::vector<FDBDistLane>& FDBDistPool::lanes() const {
    return lanes_;
}


void FDBDistPool::print(std::ostream& s) const {
    s << "FDBDistPool(name=" << name_ << ", regex=" << re_ << ", lanes=" << lanes_ << ")";
}


FDBDistLane::FDBDistLane(const eckit::PathName& home, bool writable, bool visit) :
    home_(home),
    writable_(writable),
    visit_(visit) {}


void FDBDistLane::print(std::ostream& s) const {
    s << "{" << home_ << "}";
}


//----------------------------------------------------------------------------------------------------------------------


typedef std::vector<FDBDistPool> PoolTable;
typedef std::map<eckit::PathName, PoolTable> PoolTableMap;

eckit::Mutex poolTablesMutex;
static PoolTableMap poolTables;


static void parsePoolsFile(PoolTable& table, const eckit::PathName& poolsFile) {

    if (!poolsFile.exists()) {
        std::stringstream ss;
        ss << "No pool configuration file found: " << poolsFile;
        throw eckit::UserError(ss.str(), Here());
    }

    eckit::Log::debug<LibFdb>() << "Loading dist pools from " << poolsFile << std::endl;
    std::ifstream in(poolsFile.localPath());

    eckit::Tokenizer parse(" ");

    char line[2048];
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
                const std::string& poolname = s[1];

                table.push_back(FDBDistPool(poolname, regex));
                std::cout << "New table: " << table.back() << std::endl;
                break;
            }

        default:
            eckit::Log::warning() << "FDB Pool Manager: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}


static void parseLanesFile(PoolTable& table, const eckit::PathName& lanesFile) {

    if (!lanesFile.exists()) {
        std::stringstream ss;
        ss << "No lane configuration file found: " << lanesFile;
        throw eckit::UserError(ss.str(), Here());
    }

    eckit::Log::debug<LibFdb>() << "Loading dist lanes from " << lanesFile << std::endl;
    std::ifstream in(lanesFile.localPath());

    eckit::Tokenizer parse(" ");
    eckit::Translator<std::string, bool> str2bool;

    char line[2048];
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
                const std::string& home = s[0];
                const std::string& poolname = s[1];
                const bool writable  = str2bool(s[2]);
                const bool visit = str2bool(s[3]);

                PoolTable::iterator it = table.begin();
                PoolTable::iterator end = table.end();
                for (; it != end; ++it) {
                    if (it->name() == poolname) {
                        it->appendLane(FDBDistLane(home, writable, visit));
                        std::cout << "New lane: " << it->lanes().back() << std::endl;
                        break;
                    }
                }

                if (it == end) {
                    std::stringstream ss;
                    ss << "Pool named \"" << poolname << "\" not found for lane: " << line;
                    throw eckit::UserError(ss.str(), Here());
                }
                break;
            }

        default:
            eckit::Log::warning() << "FDB Pool Manager: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}


static const PoolTable& readPoolTable(const eckit::PathName& fdbHome) {

    std::cout << "FDB home: " << fdbHome << std::endl;

    eckit::AutoLock<eckit::Mutex> lock(poolTablesMutex);

    // Table is memoised, so we only read it once.

    PoolTableMap::const_iterator it = poolTables.find(fdbHome);
    if (it != poolTables.end()) {
        return it->second;
    }

    // Parse the pools file

    eckit::PathName poolsFile = fdbHome / "etc/fdb/pools";
    PoolTable& table(poolTables[fdbHome]);
    parsePoolsFile(table, poolsFile);

    // Parse the lanes file

    eckit::PathName lanesFile = fdbHome / "etc/fdb/lanes";
    parseLanesFile(table, lanesFile);

    return table;
}


//----------------------------------------------------------------------------------------------------------------------


DistManager::DistManager(const FDBConfig& config) :
    poolTable_(readPoolTable(config.expandPath("~fdb/"))) {}


DistManager::~DistManager() {}


void DistManager::pool(const Key& key) {

    std::cout << "Getting pool for key: " << key << std::endl;

}




//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
