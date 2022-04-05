/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/FileSpace.h"

#include "eckit/os/BackTrace.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/FileSpaceStrategies.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpaceHandler.h"

using eckit::Log;

namespace eckit {
template <>
struct VectorPrintSelector<fdb5::Root> {
    typedef VectorPrintSimple selector;
};
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FileSpace::FileSpace(const std::string& name, const std::string& re, const std::string& handler,
                     const std::vector<Root>& roots) :
    name_(name), handler_(handler), re_(re), roots_(roots) {}

eckit::PathName FileSpace::filesystem(const Key& key, const eckit::PathName& db) const {
    // check that the database isn't present already
    // if it is, then return that path

    eckit::PathName root;
    if (existsDB(key, db, root)) {
        Log::debug<LibFdb5>() << "Found FDB root for key " << key << " -> " << root << std::endl;
        return root;
    }

    Log::debug<LibFdb5>() << "FDB for key " << key << " not found, selecting a root" << std::endl;
    // Log::debug<LibFdb5>() << eckit::BackTrace::dump() << std::endl;

    return FileSpaceHandler::lookup(handler_).selectFileSystem(key, *this);
}

std::vector<eckit::PathName> FileSpace::writable() const {
    std::vector<eckit::PathName> result;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->exists() and i->writable()) {
            result.push_back(i->path());
        }
    }
    return result;
}

std::vector<eckit::PathName> FileSpace::visitable() const {
    std::vector<eckit::PathName> result;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->exists() and i->visit()) {
            result.push_back(i->path());
        }
    }
    return result;
}

void FileSpace::all(eckit::StringSet& roots) const {
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->exists()) {
            roots.insert(i->path());
        }
    }
}

void FileSpace::writable(eckit::StringSet& roots) const {
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->exists() and i->writable()) {
            roots.insert(i->path());
        }
    }
}

void FileSpace::visitable(eckit::StringSet& roots) const {
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->exists() and i->visit()) {
            roots.insert(i->path());
        }
    }
}

bool FileSpace::match(const std::string& s) const {
    return re_.match(s);
}

bool FileSpace::existsDB(const Key& key, const eckit::PathName& db, eckit::PathName& root) const {
    unsigned count = 0;

    std::vector<eckit::PathName> visitables = visitable();
    std::string matchList;
    for (std::vector<eckit::PathName>::const_iterator j = visitables.begin(); j != visitables.end();
         ++j) {
        eckit::PathName fullDB = *j / db;
        if (fullDB.exists()) {
            matchList += (count == 0 ? "" : ", ") + fullDB;
            if (!count) {
                root = *j;
            }
            ++count;
        }
    }

    if (count <= 1)
        return count;

    std::ostringstream msg;
    msg << "Found multiple FDB roots matching key " << key << ", roots -> [" << matchList << "]";
    throw eckit::UserError(msg.str(), Here());
}

void FileSpace::print(std::ostream& out) const {
    out << "FileSpace("
        << "name=" << name_ << ",handler=" << handler_ << ",regex=" << re_ << ",roots=" << roots_
        << ")";
}

std::vector<eckit::PathName> FileSpace::roots() const {
    std::vector<eckit::PathName> result;
    std::transform(roots_.begin(), roots_.end(), std::back_inserter(result),
                   [](const Root& r) { return r.path(); });
    return result;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
