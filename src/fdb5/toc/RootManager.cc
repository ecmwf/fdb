/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/toc/RootManager.h"

#include "eckit/types/Types.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/parser/StringTools.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/Root.h"
#include "fdb5/toc/FileSpace.h"

using namespace eckit;

namespace fdb5 {
    class DbPathNamer;
}
namespace eckit {
    template <> struct VectorPrintSelector<fdb5::DbPathNamer> { typedef VectorPrintSimple selector; };
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DbPathNamer {
public:

    DbPathNamer(const std::string& regex, const std::string& format) :
        re_(regex),
        format_(format){
        eckit::Log::info() << "Building " << *this << std::endl;
    }

    bool match(const std::string& s) const {
        return re_.match(s);
    }

    std::string name(const Key& key, const std::string& keystr) const {

        if(format_ == "*") {
            return keystr;
        }

        eckit::StringDict tidy = key;

        return StringTools::substitute(format_, tidy);
    }

    friend std::ostream& operator<<(std::ostream &s, const DbPathNamer& x) {
        x.print(s);
        return s;
    }

private:

    void print( std::ostream &out ) const {
        out << "DbPathNamer(regex=" << re_ << ", format=" << format_ << ")";
    }

    eckit::Regex re_;

    std::string format_;

};

typedef std::vector<fdb5::DbPathNamer> DbPathNamerTable;
static DbPathNamerTable dbPathNamers;

static void readDbNamers() {

    static eckit::PathName fdbDbNamesFile = eckit::Resource<eckit::PathName>("fdbDbNamesFile;$FDB_DBNAMES_FILE", "~fdb/etc/fdb/dbnames");

    eckit::Log::info() << "Loading FDB DBPathNames from " << fdbDbNamesFile << std::endl;

    if(fdbDbNamesFile.exists()) {

        eckit::Log::debug<LibFdb>() << "Loading FDB DBPathNames from " << fdbDbNamesFile << std::endl;

        std::ifstream in(fdbDbNamesFile.localPath());

        if (!in) {
            eckit::Log::error() << fdbDbNamesFile << eckit::Log::syserr << std::endl;
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
                    const std::string& regex     = s[0];
                    const std::string& format    = s[1];

                    dbPathNamers.push_back(DbPathNamer(regex, format));
                    break;
                }

                default:
                    eckit::Log::warning() << "FDB DBPathNames invalid line ignored: " << line << std::endl;
                break;
            }
        }
    }

    dbPathNamers.push_back(DbPathNamer(".*", "*")); // always fallback to generic namer that matches all
}

//----------------------------------------------------------------------------------------------------------------------

typedef std::vector<fdb5::FileSpace> FileSpaceTable;
static FileSpaceTable spacesTable;

static std::vector<Root> readRoots() {

    std::vector<Root> result;

    static eckit::PathName fdbRootsFile = eckit::Resource<eckit::PathName>("fdbRootsFile;$FDB_ROOTS_FILE", "~fdb/etc/fdb/roots");

    std::ifstream in(fdbRootsFile.localPath());

    eckit::Log::debug<LibFdb>() << "Loading FDB roots from " << fdbRootsFile << std::endl;

    if (!in) {
        eckit::Log::error() << fdbRootsFile << eckit::Log::syserr << std::endl;
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
                const std::string& filespace  = s[1];
                bool writable        = str2bool(s[2]);
                bool visit           = str2bool(s[3]);

                result.push_back(Root(path, filespace, writable, visit));
                break;
            }

        default:
            eckit::Log::warning() << "FDB RootManager: Invalid line ignored: " << line << std::endl;
            break;
        }
    }

    return result;
}

static std::vector<Root> fileSpaceRoots(const std::vector<Root>& all, const std::string& filespace) {

    std::vector<Root> roots;

    for (std::vector<Root>::const_iterator i = all.begin(); i != all.end() ; ++i) {
        if (i->filespace() == filespace) {
            roots.push_back(*i);
        }
    }
    return roots;
}

static void readFileSpaces() {

    std::vector<Root> allRoots = readRoots();

    static eckit::PathName fdbSpacesFile = eckit::Resource<eckit::PathName>("fdbSpacesFile;$FDB_SPACES_FILE", "~fdb/etc/fdb/spaces");

    std::ifstream in(fdbSpacesFile.localPath());

    eckit::Log::debug<LibFdb>() << "Loading FDB file spaces from " << fdbSpacesFile << std::endl;

    if (!in) {
        eckit::Log::error() << fdbSpacesFile << eckit::Log::syserr << std::endl;
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
            case 3: {
                const std::string& regex     = s[0];
                const std::string& filespace = s[1];
                const std::string& handler   = s[2];

                std::vector<Root> roots = fileSpaceRoots(allRoots, filespace);

                if(!roots.size()) {
                    std::ostringstream oss;
                    oss << "No roots found for filespace " << filespace;
                    throw UserError(oss.str(), Here());
                }

                spacesTable.push_back(FileSpace(filespace, regex, handler, roots));
                break;
            }

        default:
            eckit::Log::warning() << "FDB RootManager: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

static pthread_once_t once = PTHREAD_ONCE_INIT;

static void init() {
    readFileSpaces();
    readDbNamers();
}

//----------------------------------------------------------------------------------------------------------------------

PathName RootManager::dbPathName(const Key& key, const std::string& keystr)
{
    PathName dbpath;
    for (DbPathNamerTable::const_iterator i = dbPathNamers.begin(); i != dbPathNamers.end() ; ++i) {
        if(i->match(keystr)) {
            dbpath = i->name(key, keystr);
            eckit::Log::debug<LibFdb>() << "DbName is " << dbpath << " for key " << key <<  std::endl;
            return dbpath;
        }
    }

    std::ostringstream msg;
    msg << "No dbNamer matches key " << key << " from list of dbNamers " << dbPathNamers;
    throw UserError(msg.str());
}

eckit::PathName RootManager::directory(const Key& key) {

    pthread_once(&once, init);

    eckit::Log::debug<LibFdb>() << "Choosing directory for key " << key << std::endl;

    std::string keystr(key.valuesToString());

    PathName dbpath = dbPathName(key, keystr);

    // override root location for testing purposes only

    static std::string fdbRootDirectory = eckit::Resource<std::string>("fdbRootDirectory;$FDB_ROOT_DIRECTORY", "");

    if(!fdbRootDirectory.empty()) {
        return fdbRootDirectory + "/" + dbpath;
    }

    // returns the first filespace that matches

    for (FileSpaceTable::const_iterator i = spacesTable.begin(); i != spacesTable.end() ; ++i) {
        if(i->match(keystr)) {
            PathName root = i->filesystem(key);
            eckit::Log::debug<LibFdb>() << "Directory is " << root / dbpath <<  std::endl;
            return root / dbpath;
        }
    }

    std::ostringstream oss;
    oss << "No FDB file space for " << key << " (" << keystr << ")";
    throw eckit::SeriousBug(oss.str());
}

std::vector<PathName> RootManager::allRoots(const Key& key)
{
    eckit::StringSet roots;

    pthread_once(&once, init);

    std::string k = key.valuesToString();

    for (FileSpaceTable::const_iterator i = spacesTable.begin(); i != spacesTable.end() ; ++i) {
        if(i->match(k)) {
            i->all(roots);
        }
    }

    return std::vector<eckit::PathName>(roots.begin(), roots.end());
}


std::vector<eckit::PathName> RootManager::visitableRoots(const Key& key) {

    eckit::StringSet roots;

    pthread_once(&once, init);

    std::string k = key.valuesToString();

    for (FileSpaceTable::const_iterator i = spacesTable.begin(); i != spacesTable.end() ; ++i) {
        if(i->match(k)) {
            i->visitable(roots);
        }
    }

    Log::debug<LibFdb>() << "Visitable Roots " << roots << std::endl;

    return std::vector<eckit::PathName>(roots.begin(), roots.end());
}

std::vector<eckit::PathName> RootManager::writableRoots(const Key& key) {

    eckit::StringSet roots;

    pthread_once(&once, init);

    std::string k = key.valuesToString();

    for (FileSpaceTable::const_iterator i = spacesTable.begin(); i != spacesTable.end() ; ++i) {
        if(i->match(k)) {
            i->writable(roots);
        }
    }

    return std::vector<eckit::PathName>(roots.begin(), roots.end());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
