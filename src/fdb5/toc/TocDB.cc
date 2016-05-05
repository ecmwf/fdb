/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/toc/TocDB.h"
#include "fdb5/rules/Rule.h"
#include "eckit/log/Timer.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Regex.h"
#include "fdb5/config/MasterConfig.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key) :
    DB(key),
    TocHandler(directory(key)) {
}

TocDB::TocDB(const eckit::PathName& directory) :
    DB(Key()),
    TocHandler(directory) {
}

TocDB::~TocDB() {
}

void TocDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis() not implemented for " << *this << std::endl;
    NOTIMP;
}

bool TocDB::open() {
    Log::error() << "Open not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::archive(const Key &key, const void *data, Length length) {
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::flush() {
    Log::error() << "Flush not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle *TocDB::retrieve(const Key &key) const {
    Log::error() << "Retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::close() {
    Log::error() << "Close not implemented for " << *this << std::endl;
    NOTIMP;
}

void TocDB::loadSchema() {
    Timer timer("TocDB::loadSchema()");
    schema_.load( directory_ / "schema" );
}

void TocDB::checkSchema(const Key &key) const {
    Timer timer("TocDB::checkSchema()");
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
}

const Schema& TocDB::schema() const {
    return schema_;
}

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

static pthread_once_t once = PTHREAD_ONCE_INIT;
static std::vector< std::pair<eckit::Regex, std::string> > rootsTable;

static void readTable() {
    eckit::PathName path("~/etc/fdb/roots");
    std::ifstream in(path.localPath());

    eckit::Log::info() << "Loading FDB roots from " << path << std::endl;

    if (in.bad()) {
        eckit::Log::error() << path << eckit::Log::syserr << std::endl;
        return;
    }

    char line[1024];
    while (in.getline(line, sizeof(line))) {
        eckit::Tokenizer parse(" ");
        std::vector<std::string> s;
        parse(line, s);

        // Log::info() << "FDB table " << line << std::endl;

        size_t i = 0;
        while (i < s.size()) {
            if (s[i].length() == 0)
                s.erase(s.begin() + i);
            else
                i++;
        }

        if (s.size() == 0 || s[0][0] == '#')
            continue;

        switch (s.size()) {
        case 2:
            rootsTable.push_back(std::make_pair(eckit::Regex(s[0]), s[1]));
            break;

        default:
            eckit::Log::warning() << "FDB Root: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

eckit::PathName TocDB::directory(const Key &key) {

    /// @note This may not be needed once in operations, but helps with testing tools
    static std::string overideRoot = eckit::Resource<std::string>("$FDB_ROOT", "");

    std::string root( overideRoot );

    if (root.empty()) {
        static eckit::StringList fdbRootPattern( eckit::Resource<eckit::StringList>("fdbRootPattern", "class,stream,expver", true ) );
        pthread_once(&once, readTable);

        std::ostringstream oss;

        const char *sep = "";
        for (eckit::StringList::const_iterator j = fdbRootPattern.begin(); j != fdbRootPattern.end(); ++j) {
            Key::const_iterator i = key.find(*j);
            if (i == key.end()) {
                oss << sep << "unknown";
                eckit::Log::warning() << "FDB root: cannot get " << *j << " from " << key << std::endl;
            } else {
                oss << sep << key.get(*j);
            }
            sep = ":";
        }

        std::string name(oss.str());

        for (size_t i = 0; i < rootsTable.size() ; i++)
            if (rootsTable[i].first.match(name)) {
                root = rootsTable[i].second;
                break;
            }

        if (root.length() == 0) {
            std::ostringstream oss;
            oss << "No FDB root for " << key;
            throw eckit::SeriousBug(oss.str());
        }
    }

    return eckit::PathName(root) / key.valuesToString();
}

std::vector<eckit::PathName> TocDB::databases(const Key &key) {

    const Schema& schema = MasterConfig::instance().schema();
    std::set<Key> keys;
    schema.matchFirstLevel(key, keys);

    std::vector<eckit::PathName> dirs = roots(); // TODO: filter roots() with key
    std::vector<eckit::PathName> result;
    std::set<eckit::PathName> seen;

    for (std::vector<eckit::PathName>::const_iterator j = dirs.begin(); j != dirs.end(); ++j) {

        std::vector<eckit::PathName> subdirs;
        eckit::PathName::match((*j) / "*:*", subdirs, false);

        for (std::set<Key>::const_iterator i = keys.begin(); i != keys.end(); ++i) {


            Regex re("^" + (*i).valuesToString() + "$");
            // std::cout << re << std::endl;


            for (std::vector<eckit::PathName>::const_iterator k = subdirs.begin(); k != subdirs.end(); ++k) {

                if(seen.find(*k) != seen.end()) {
                    continue;
                }

                if (re.match((*k).baseName())) {
                    try {
                        TocHandler toc(*k);
                        if (toc.databaseKey().match(key)) {
                            result.push_back(*k);
                        }
                    } catch (eckit::Exception& e) {
                        eckit::Log::error() <<  "Error loading FDB database from " << *k << std::endl;
                        eckit::Log::error() << e.what() << std::endl;
                    }
                    seen.insert(*k);;
                }

            }
        }
    }

    return result;
}

std::vector<eckit::PathName> TocDB::roots(const std::string& match) {

    static std::string overideRoot = eckit::Resource<std::string>("$FDB_ROOT", "");
    std::string root( overideRoot );

    eckit::StringSet roots;

    if (!root.empty()) {
        roots.insert(root);
    } else {
        pthread_once(&once, readTable);

        for (size_t i = 0; i < rootsTable.size() ; i++) {
            if (rootsTable[i].first.match(match)) {
                roots.insert(rootsTable[i].second);
            }
        }
    }

    return std::vector<eckit::PathName>(roots.begin(), roots.end());
}

} // namespace fdb5
