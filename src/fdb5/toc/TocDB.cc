/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/log/Timer.h"
#include "eckit/parser/Tokenizer.h"
#include "eckit/utils/Regex.h"

#include "fdb5/toc/TocDB.h"
#include "fdb5/rules/Rule.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static pthread_once_t once = PTHREAD_ONCE_INIT;
static std::vector< std::pair<Regex, std::string> > rootsTable;

static void readTable() {
    eckit::PathName path("~/etc/fdb/roots");
    std::ifstream in(path.localPath());

    Log::info() << "FDB table " << path << std::endl;

    if (in.bad()) {
        Log::error() << path << Log::syserr << std::endl;
        return;
    }

    char line[1024];
    while (in.getline(line, sizeof(line))) {
        Tokenizer parse(" ");
        std::vector<std::string> s;
        parse(line, s);

        Log::info() << "FDB table " << line << std::endl;

        Ordinal i = 0;
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
            rootsTable.push_back(std::make_pair(Regex(s[0]), s[1]));
            break;

        default:
            Log::warning() << "FDB Root: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

static eckit::PathName directory(const Key &key) {

    /// @note This may not be needed once in operations, but helps with testing tools
    static std::string overideRoot = eckit::Resource<std::string>("$FDB_ROOT", "");

    std::string root( overideRoot );

    if(root.empty())
    {
        static StringList fdbRootPattern( eckit::Resource<StringList>("fdbRootPattern", "class,stream,expver", true ) );
        pthread_once(&once, readTable);

        std::ostringstream oss;

        const char *sep = "";
        for (StringList::const_iterator j = fdbRootPattern.begin(); j != fdbRootPattern.end(); ++j) {
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

        for (Ordinal i = 0; i < rootsTable.size() ; i++)
            if (rootsTable[i].first.match(name)) {
                root = rootsTable[i].second;
                break;
            }

        if (root.length() == 0) {
            std::ostringstream oss;
            oss << "No FDB root for " << key;
            throw SeriousBug(oss.str());
        }
    }

    return PathName(root) / key.valuesToString();
}

//----------------------------------------------------------------------------------------------------------------------

TocDB::TocDB(const Key& key) :
    DB(key),
    TocHandler(directory(key)) {
}

TocDB::TocDB(const eckit::PathName& directory) :
    DB(Key()),
    TocHandler(directory)
{
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

const Schema& TocDB::schema() const
{
    return schema_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
