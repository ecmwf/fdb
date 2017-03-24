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

#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Translator.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/Root.h"
#include "fdb5/toc/FileSpace.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

typedef std::vector<fdb5::FileSpace> FileSpaceTable;

static pthread_once_t once = PTHREAD_ONCE_INIT;
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

eckit::PathName RootManager::directory(const Key& key) {

    pthread_once(&once, readFileSpaces);
            
    eckit::Log::debug<LibFdb>() << "Choosing directory for key " << key << std::endl;

    std::string name(key.valuesToString());

    // override root location for testting purposes only

    static std::string fdbRootDirectory = eckit::Resource<std::string>("fdbRootDirectory;$FDB_ROOT_DIRECTORY", "");

    if(!fdbRootDirectory.empty()) {
        return fdbRootDirectory + "/" + name;
    }

    // returns the first filespace that matches

    for (FileSpaceTable::const_iterator i = spacesTable.begin(); i != spacesTable.end() ; ++i) {
        if(i->match(name)) {
            PathName path = i->filesystem(key);
            eckit::Log::debug<LibFdb>() << "Directory is " << path / name <<  std::endl;
            return path / name;
        }
    }

    std::ostringstream oss;
    oss << "No FDB file space for " << key << " (" << name << ")";
    throw eckit::SeriousBug(oss.str());
}

std::vector<PathName> RootManager::allRoots(const Key& key)
{
    eckit::StringSet roots;

    pthread_once(&once, readFileSpaces);

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

    pthread_once(&once, readFileSpaces);

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

    pthread_once(&once, readFileSpaces);

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
