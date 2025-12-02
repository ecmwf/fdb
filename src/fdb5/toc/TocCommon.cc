/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocCommon.h"

#include <pwd.h>
#include <unistd.h>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/URIManager.h"
#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/io/LustreSettings.h"
#include "fdb5/toc/RootManager.h"

namespace fdb5 {

eckit::LocalPathName TocCommon::findRealPath(const eckit::LocalPathName& path) {

    // realpath only works on existing paths, so work back up the path until
    // we find one that does, get the realpath on that, then reconstruct.
    if (path.exists()) {
        return path.realName();
    }

    return findRealPath(path.dirName()) / path.baseName();
}

TocCommon::TocCommon(const eckit::PathName& directory) :
    directory_(findRealPath(eckit::LocalPathName{directory})),
    schemaPath_(directory_ / "schema"),
    dbUID_(static_cast<uid_t>(-1)),
    userUID_(::getuid()) {}

void TocCommon::checkUID() const {
    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers =
        eckit::Resource<std::vector<std::string>>("fdbSuperUsers", "", true);

    if (dbUID() != userUID_) {
        if (std::find(fdbSuperUsers.begin(), fdbSuperUsers.end(), userName(userUID_)) == fdbSuperUsers.end()) {
            std::ostringstream oss;
            oss << "Only user '" << userName(dbUID()) << "' can write to FDB " << directory_ << ", current user is '"
                << userName(userUID_) << "'";

            throw eckit::UserError(oss.str());
        }
    }
}

uid_t TocCommon::dbUID() const {
    if (dbUID_ == static_cast<uid_t>(-1)) {
        // TODO: Do properly in eckit
        struct stat s;
        SYSCALL(::stat(directory_.localPath(), &s));
        dbUID_ = s.st_uid;
    }

    return dbUID_;
}

std::string TocCommon::userName(uid_t uid) {
    struct passwd* p = getpwuid(uid);

    if (p) {
        return p->pw_name;
    }
    else {
        return eckit::Translator<long, std::string>()(uid);
    }
}

}  // namespace fdb5