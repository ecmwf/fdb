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
#include "fdb5/toc/TocHandler.h"

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

TocPath FileSpace::filesystem(const Key& key, const eckit::PathName& db) const {
    // check that the database isn't present already
    // if it is, then return that path

    TocPath root;
    if (existsDB(key, db, root)) {
        Log::debug<LibFdb5>() << "Found FDB root for key " << key << " -> " << root.directory_ << std::endl;
        return root;
    }

    Log::debug<LibFdb5>() << "FDB for key " << key << " not found, selecting a root" << std::endl;
    // Log::debug<LibFdb5>() << eckit::BackTrace::dump() << std::endl;

    return TocPath{FileSpaceHandler::lookup(handler_).selectFileSystem(key, *this), ControlIdentifiers{}};
}

std::vector<eckit::PathName> FileSpace::enabled(const ControlIdentifier& controlIdentifier) const {
    std::vector<eckit::PathName> result;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->enabled(controlIdentifier) && i->exists()) {
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

void FileSpace::enabled(const ControlIdentifier& controlIdentifier, eckit::StringSet& roots) const {
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->enabled(controlIdentifier) && i->exists()) {
            roots.insert(i->path());
        }
    }
}

bool FileSpace::match(const std::string& s) const {
    return re_.match(s);
}

bool FileSpace::existsDB(const Key& key, const eckit::PathName& db, TocPath& root) const {
    unsigned count = 0;
    bool found = false;

//    std::vector<const Root&> visitables = visitable();
    std::string matchList;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->enabled(ControlIdentifier::List) && i->exists()) {
            eckit::PathName fullDB = i->path() / db;
            eckit::PathName dbToc = i->path() / db / "toc";
            if (fullDB.exists() && dbToc.exists()) {
                matchList += (count == 0 ? "" : ", ") + fullDB;

                bool allowMultipleDbs = (fullDB / (controlfile_lookup.find(ControlIdentifier::UniqueRoot)->second)).exists();
                if (!count || allowMultipleDbs) { // take last
                    root.directory_ = i->path();
                    root.controlIdentifiers_ = i->controlIdentifiers();
                    found = true;
                }
                if (!allowMultipleDbs)
                    ++count;
            }
        }
    }

    if (count <= 1)
        return found;

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
