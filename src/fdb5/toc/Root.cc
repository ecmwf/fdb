/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/toc/Root.h"
#include "fdb5/database/Key.h"
#include "eckit/config/Resource.h"
#include "eckit/parser/Tokenizer.h"


using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


Root::Root(const std::string &re, const std::string &path, bool active, bool visit):
    re_(re),
    path_(path),
    active_(active),
    visit_(visit) {

}

bool Root::match(const std::string &s) const {
    return re_.match(s);
}

const eckit::PathName &Root::path() const {
    return path_;
}

bool Root::active() const {
    return active_;  // Root is in use, when archiving
}

bool Root::visit() const {
    return visit_;  // Root is visited, when retrievind
}


//----------------------------------------------------------------------------------------------------------------------

static pthread_once_t once = PTHREAD_ONCE_INIT;
static std::vector<Root> rootsTable;

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
            rootsTable.push_back(Root(s[0], s[1]));
            break;

        default:
            eckit::Log::warning() << "FDB Root: Invalid line ignored: " << line << std::endl;
            break;

        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

eckit::PathName Root::directory(const Key &key) {

    pthread_once(&once, readTable);

    std::string name(key.valuesToString());

    for (std::vector<Root>::const_iterator i = rootsTable.begin(); i != rootsTable.end() ; ++i) {
        if (i->active() && i->match(name)) {
            return  i->path() / name;
        }
    }

    std::ostringstream oss;
    oss << "No FDB root for " << key << " (" << name << ")";
    throw eckit::SeriousBug(oss.str());

}

std::vector<eckit::PathName> Root::roots(const std::string &match) {

    eckit::StringSet roots;

    pthread_once(&once, readTable);

    for (std::vector<Root>::const_iterator i = rootsTable.begin(); i != rootsTable.end() ; ++i) {
        if (i->visit() && i->match(match)) {
            roots.insert(i->path());
        }
    }

    return std::vector<eckit::PathName>(roots.begin(), roots.end());
}

} // namespace fdb5
