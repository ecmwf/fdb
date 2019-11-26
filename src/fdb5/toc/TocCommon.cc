/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <pwd.h>
#include <unistd.h>

//#include "eckit/config/Resource.h"
#include "eckit/filesystem/URIManager.h"
#include "eckit/log/Timer.h"

#include "fdb5/io/LustreFileHandle.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCommon.h"

namespace fdb5 {

eckit::PathName TocCommon::findRealPath(const eckit::PathName& path) {

    // realpath only works on existing paths, so work back up the path until
    // we find one that does, get the realpath on that, then reconstruct.
    if (path.exists()) return path.realName();

    return findRealPath(path.dirName()) / path.baseName();
}

eckit::PathName TocCommon::getDirectory(const Key& key, const Config& config) {

    return RootManager(config).directory(key);
}

TocCommon::TocCommon(const eckit::PathName& directory) :
    directory_(findRealPath(directory)),
    dbUID_(-1),
    userUID_(::getuid()),
    schemaPath_(directory_ / "schema"),
//    tocPath_(directory_ / "toc"),
//    useSubToc_(config.getBool("useSubToc", false)),
//    isSubToc_(false),
//    fd_(-1),
//    cachedToc_(nullptr),
//    count_(0),
//    enumeratedMaskedEntries_(false),
//    writeMode_(false),
    dirty_(false) {
}


void TocCommon::checkUID() const {
    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers =
        eckit::Resource<std::vector<std::string> >("fdbSuperUsers", "", true);

    if (dbUID() != userUID_) {
        if (std::find(fdbSuperUsers.begin(), fdbSuperUsers.end(), userName(userUID_)) ==
            fdbSuperUsers.end()) {
            std::ostringstream oss;
            oss << "Only user '" << userName(dbUID())
                << "' can write to FDB " << directory_ << ", current user is '"
                << userName(userUID_) << "'";

            throw eckit::UserError(oss.str());
        }
    }
}

long TocCommon::dbUID() const {
    if (dbUID_ == -1)
        dbUID_ = directory_.owner();

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

bool TocCommon::stripeLustre() {
    static bool handleLustreStripe = eckit::Resource<bool>("fdbHandleLustreStripe;$FDB_HANDLE_LUSTRE_STRIPE", true);
    return fdb5LustreapiSupported() && handleLustreStripe;
}

class FdbFileURIManager : public eckit::URIManager {
    virtual bool query() override { return true; }
    virtual bool fragment() override { return true; }

    virtual bool exists(const eckit::URI& f) override { return eckit::PathName(f.name()).exists(); }

    virtual eckit::DataHandle* newWriteHandle(const eckit::URI& f) override { return eckit::PathName(f.name()).fileHandle(); }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f) override { return eckit::PathName(f.name()).fileHandle(); }

    virtual eckit::DataHandle* newReadHandle(const eckit::URI& f, const eckit::OffsetList& ol, const eckit::LengthList& ll) override {
        return eckit::PathName(f.name()).partHandle(ol, ll);
    }

    virtual std::string asString(const eckit::URI& uri) const override {
        std::string q = uri.query();
        if (!q.empty())
            q = "?" + q;
        std::string f = uri.fragment();
        if (!f.empty())
            f = "#" + f;

        return uri.name() + q + f;
    }
public:
    FdbFileURIManager(const std::string& name) : eckit::URIManager(name) {}
};

static FdbFileURIManager manager_fdb_file("fdbfile");

}
