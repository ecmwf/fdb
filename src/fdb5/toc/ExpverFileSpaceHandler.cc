/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "ExpverFileSpaceHandler.h"

#include <sys/file.h>
#include <cctype>

#include <algorithm>
#include <fstream>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileLock.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/utils/Literals.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpace.h"

using namespace eckit;
using namespace eckit::literals;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ExpverFileSpaceHandler::ExpverFileSpaceHandler(const Config& config) :
    FileSpaceHandler(config),
    fdbExpverFileSystems_(config.expandPath(eckit::Resource<std::string>("fdbExpverFileSystems;$FDB_EXPVER_FILE",
                                                                         "~fdb/etc/fdb/expver_to_fdb_root.map"))) {}

ExpverFileSpaceHandler::~ExpverFileSpaceHandler() {}

void ExpverFileSpaceHandler::load() const {

    LOG_DEBUG_LIB(LibFdb5) << "Loading " << fdbExpverFileSystems_ << std::endl;

    std::ifstream in(fdbExpverFileSystems_.localPath());

    if (!in) {
        std::ostringstream oss;
        oss << fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw CantOpenFile(oss.str(), Here());
    }

    char line[1_KiB];
    size_t lineNo = 0;
    Tokenizer parse(" ");
    std::vector<std::string> s;

    while (in.getline(line, sizeof(line))) {
        ++lineNo;
        s.clear();

        parse(line, s);

        size_t i = 0;
        while (i < s.size()) /* cleanup entries that are empty */
        {
            if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            }
            else {
                i++;
            }
        }

        if (s.size() == 0 || s[0][0] == '#') {
            continue;
        }

        if (s.size() != 2) {
            std::ostringstream oss;
            oss << "Bad line (" << lineNo << ") in configuration file " << fdbExpverFileSystems_
                << " -- should have format 'expver filesystem'";
            throw ReadError(oss.str(), Here());
        }

        table_[s[0]] = PathName(s[1]);
    }
}

eckit::PathName ExpverFileSpaceHandler::append(const std::string& expver, const PathName& path) const {
    // obtain exclusive lock to file

    PathName lockFile = fdbExpverFileSystems_ + ".lock";

    eckit::FileLock locker(lockFile);
    eckit::AutoLock<eckit::FileLock> lock(locker);

    // read the file first to check that this expver hasn't been inserted yet by another process

    std::ifstream fi(fdbExpverFileSystems_.localPath());

    if (!fi) {
        std::ostringstream oss;
        oss << fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw CantOpenFile(oss.str(), Here());
    }

    char line[4_KiB];
    size_t lineNo = 0;
    Tokenizer parse(" ");
    std::vector<std::string> s;

    while (fi.getline(line, sizeof(line))) {
        ++lineNo;
        s.clear();

        parse(line, s);

        size_t i = 0;
        while (i < s.size()) /* cleanup entries that are empty */
        {
            if (s[i].length() == 0) {
                s.erase(s.begin() + i);
            }
            else {
                i++;
            }
        }

        if (s.size() == 0 || s[0][0] == '#') {
            continue;
        }

        if (s.size() != 2) {
            std::ostringstream oss;
            oss << "Bad line (" << lineNo << ") in configuration file " << fdbExpverFileSystems_
                << " -- should have format 'expver filesystem'";
            throw ReadError(oss.str(), Here());
        }

        if (s[0] == expver) {
            LOG_DEBUG_LIB(LibFdb5) << "Found expver " << expver << " " << path << " in " << fdbExpverFileSystems_
                                   << std::endl;
            return PathName(s[1]);
        }
    }

    fi.close();

    std::ofstream of(fdbExpverFileSystems_.localPath(), std::ofstream::app);

    if (!of) {
        std::ostringstream oss;
        oss << fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw WriteError(oss.str(), Here());
    }

    // append to the file

    LOG_DEBUG_LIB(LibFdb5) << "Appending expver " << expver << " " << path << " to " << fdbExpverFileSystems_
                           << std::endl;

    of << expver << " " << path << std::endl;

    of.close();

    return path;
}

PathName ExpverFileSpaceHandler::select(const Key& key, const FileSpace& fs) const {
    return FileSpaceHandler::lookup("WeightedRandom", config_).selectFileSystem(key, fs);
}

static bool expver_is_valid(const std::string& str) {
    LOG_DEBUG_LIB(LibFdb5) << "Validating expver string [" << str << "]" << std::endl;
    return ((str.size() <= 4) and std::find_if_not(str.begin(), str.end(), isdigit) == str.end()) ||
           ((str.size() == 4) and std::find_if_not(str.begin(), str.end(), isalnum) == str.end());
}

eckit::PathName ExpverFileSpaceHandler::selectFileSystem(const Key& key, const FileSpace& fs) const {

    AutoLock<Mutex> lock(mutex_);

    // Has the user specified a root to use already?

    static std::string fdbRootDirectory = eckit::Resource<std::string>("fdb5Root;$FDB5_ROOT", "");

    // check if key is mapped already to a filesystem

    if (table_.empty()) {
        load();
    }

    std::string expver = key.get("expver");

    // we can NOT use the type system here because we haven't opened a DB yet
    // so we have to do validation directly on string
    if (not expver_is_valid(expver)) {
        throw eckit::BadValue("Invalid expver value " + expver, Here());
    }

    LOG_DEBUG_LIB(LibFdb5) << "Selecting file system for expver [" << expver << "]" << std::endl;

    PathTable::const_iterator itr = table_.find(expver);
    if (itr != table_.end()) {
        LOG_DEBUG_LIB(LibFdb5) << "Found expver " << expver << " " << itr->second << " in " << fdbExpverFileSystems_
                               << std::endl;

        if (!fdbRootDirectory.empty() && itr->second != fdbRootDirectory) {
            Log::warning() << "Existing root directory " << itr->second << " does not match FDB5_ROOT. Using existing"
                           << std::endl;
        }

        return itr->second;
    }

    // if not, assign a filesystem. Use the algorithm in select by default, unless overridden.

    PathName maybe;
    if (fdbRootDirectory.empty()) {
        maybe = select(key, fs);
    }
    else {
        // Before we allow an override, ensure that it is one of the available filesystems.
        std::vector<PathName> writable(fs.enabled(ControlIdentifier::Archive));

        if (std::find(writable.begin(), writable.end(), fdbRootDirectory) == writable.end()) {
            std::ostringstream msg;
            msg << "FDB root directory " << fdbRootDirectory << " was not in the list of roots supporting archival "
                << writable;
            throw BadParameter(msg.str());
        }

        LOG_DEBUG_LIB(LibFdb5) << "Using root directory specified by FDB5_ROOT: " << fdbRootDirectory << std::endl;
        maybe = fdbRootDirectory;
    }

    // Append will add the value of maybe to the table file, unless another process beats us there. If it
    // does, the value from the other process will be returned (and used).

    PathName selected = append(expver, maybe);

    table_[expver] = selected;

    if (!fdbRootDirectory.empty() && selected != fdbRootDirectory) {
        Log::warning() << "Selected root directory " << itr->second << " does not match FDB5_ROOT. Using existing"
                       << std::endl;
    }

    return selected;
}

static FileSpaceHandlerRegister<ExpverFileSpaceHandler> expver("expver");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
