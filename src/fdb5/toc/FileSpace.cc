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
#include "eckit/config/LocalConfiguration.h"
#include "eckit/config/Resource.h"
#include "eckit/filesystem/StdDir.h"
#include "eckit/utils/StringTools.h"

#include "eckit/exception/Exceptions.h"

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
// MatchSelector

FileSpace::MatchSelector::MatchSelector(const eckit::LocalConfiguration& cfg) {
    for (const auto& keyword : cfg.keys()) {
        std::vector<std::string> values;
        if (cfg.isList(keyword)) {
            values = cfg.getStringVector(keyword);
        }
        else {
            values.emplace_back(cfg.getString(keyword));
        }
        // empty values would be meaningless (would never match a present keyword)
        if (values.empty()) {
            std::ostringstream oss;
            oss << "FileSpace match: keyword '" << keyword << "' has no values";
            throw eckit::UserError(oss.str(), Here());
        }
        entries_.emplace(keyword, std::move(values));
    }
}

bool FileSpace::MatchSelector::match(const Key& key) const {
    for (const auto& [keyword, allowed] : entries_) {
        const auto [iter, found] = key.find(keyword);
        if (!found || iter->second.empty()) {
            // FDB-331: a keyword absent from the request is simply ambiguous on that dimension
            // and we let the caller visit the space.
            continue;
        }
        const auto& value = iter->second;
        bool ok = false;
        for (const auto& candidate : allowed) {
            if (candidate == value) {
                ok = true;
                break;
            }
        }
        if (!ok) {
            return false;
        }
    }
    return true;
}

void FileSpace::MatchSelector::print(std::ostream& s) const {
    s << "{";
    const char* sep = "";
    for (const auto& [keyword, allowed] : entries_) {
        s << sep << keyword << "=";
        if (allowed.size() == 1) {
            s << allowed.front();
        }
        else {
            s << "[";
            const char* vsep = "";
            for (const auto& value : allowed) {
                s << vsep << value;
                vsep = ",";
            }
            s << "]";
        }
        sep = ",";
    }
    s << "}";
}

//----------------------------------------------------------------------------------------------------------------------
// FileSpace

FileSpace::FileSpace(const std::string& name, const std::string& re, const std::string& handler,
                     const std::vector<Root>& roots) :
    name_(name), handler_(handler), re_(re), roots_(roots) {}

FileSpace::FileSpace(const std::string& name, const MatchSelector& matcher, const std::string& handler,
                     const std::vector<Root>& roots) :
    name_(name), handler_(handler), matcher_(matcher), roots_(roots) {}

TocPath FileSpace::filesystem(const Config& config, const Key& key, const eckit::PathName& db) const {
    // check that the database isn't present already
    // if it is, then return that path

    TocPath existingDB;
    if (existsDB(key, db, existingDB)) {
        LOG_DEBUG_LIB(LibFdb5) << "Found existing DB for key " << key << " -> " << existingDB.directory_ << std::endl;
        return existingDB;
    }

    LOG_DEBUG_LIB(LibFdb5) << "FDB for key " << key << " not found, selecting a root" << std::endl;
    LOG_DEBUG_LIB(LibFdb5) << "FDB for key " << key << " not found, selecting a root" << std::endl;

    return TocPath{FileSpaceHandler::lookup(handler_, config).selectFileSystem(key, *this) / db, ControlIdentifiers{}};
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

bool FileSpace::match(const Key& key) const {
    if (matcher_) {
        return matcher_->match(key);
    }
    return re_.match(key.valuesToString());
}

eckit::PathName getFullDB(const eckit::PathName& path, const std::string& db) {

    static bool searchCaseSensitiveDB =
        eckit::Resource<bool>("fdbSearchCaseSensitiveDB;$FDB_SEARCH_CASESENSITIVE_DB", true);

    if (searchCaseSensitiveDB) {

        eckit::StdDir d(path.path().c_str());
        if (d == nullptr) {
            Log::error() << "opendir(" << path << ")" << Log::syserr << std::endl;
            throw eckit::FailedSystemCall("opendir");
        }

        // Once readdir_r finally gets deprecated and removed, we may need to
        // protecting readdir() as not yet guarranteed thread-safe by POSIX
        // technically it should only be needed on a per-directory basis
        // this should be a resursive mutex
        // AutoLock<Mutex> lock(mutex_);
        std::string ldb = eckit::StringTools::lower(db);

        for (;;) {
            struct dirent* e = d.dirent();
            if (e == nullptr) {
                break;
            }

            if (ldb == eckit::StringTools::lower(e->d_name)) {
                return path / e->d_name;
            }
        }
    }

    return path / db;
}


bool FileSpace::existsDB(const Key& key, const eckit::PathName& db, TocPath& existsDB) const {
    unsigned count = 0;
    bool found = false;

    std::string matchList;
    for (RootVec::const_iterator i = roots_.begin(); i != roots_.end(); ++i) {
        if (i->enabled(ControlIdentifier::List) && i->exists()) {
            eckit::PathName fullDB = getFullDB(i->path(), db);
            eckit::PathName dbToc = fullDB / "toc";
            if (fullDB.exists() && dbToc.exists()) {
                matchList += (count == 0 ? "" : ", ") + fullDB;

                bool allowMultipleDbs =
                    (fullDB / (controlfile_lookup.find(ControlIdentifier::UniqueRoot)->second)).exists();
                if (!count || allowMultipleDbs) {  // take last
                    existsDB.directory_ = fullDB;
                    existsDB.controlIdentifiers_ = i->controlIdentifiers();
                    found = true;
                }
                if (!allowMultipleDbs) {
                    ++count;
                }
            }
        }
    }

    if (count <= 1) {
        return found;
    }

    std::ostringstream msg;
    msg << "Found multiple FDB roots matching key " << key << ", roots -> [" << matchList << "]";
    throw eckit::UserError(msg.str(), Here());
}

void FileSpace::print(std::ostream& out) const {
    out << "FileSpace("
        << "name=" << name_ << ",handler=" << handler_;
    if (matcher_) {
        out << ",match=" << *matcher_;
    }
    else {
        out << ",regex=" << re_;
    }
    out << ",roots=" << roots_ << ")";
}

std::vector<eckit::PathName> FileSpace::roots() const {
    std::vector<eckit::PathName> result;
    std::transform(roots_.begin(), roots_.end(), std::back_inserter(result), [](const Root& r) { return r.path(); });
    return result;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
