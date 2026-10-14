/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/detail/TocLocking.h"

#include <memory>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/toc/detail/NFSTocLocking.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// O_APPEND is atomic everywhere but on NFS, so there is nothing to lock.
class NoTocLocking final : public TocLocking {
public:

    void validate(const eckit::LocalPathName& /*directory*/) const override {}

    std::unique_ptr<TocLock> lockForRead(int /*fd*/, const eckit::PathName& /*path*/) const override { return nullptr; }

    std::unique_ptr<TocLock> lockForWrite(int /*fd*/, const eckit::PathName& /*path*/) const override {
        return nullptr;
    }

    bool excludesWriters() const override { return false; }

    eckit::PathName uniquePath(const eckit::PathName& path) const override { return eckit::PathName::unique(path); }
};

/// False where the platform cannot tell the filesystem type.
bool isOnNFS(const eckit::LocalPathName& directory) {
    try {
        return directory.fileSystemType() == "nfs";
    }
    catch (const eckit::NotImplemented&) {
        return false;
    }
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

eckit::LocalPathName TocLocking::locateExistingDirectory(const eckit::LocalPathName& path) {
    eckit::LocalPathName parent = path;
    while (!parent.exists()) {
        parent = parent.dirName();  // ends at "/" or ".", which exist
    }
    return parent;
}

const TocLocking& TocLocking::forDirectory(const eckit::LocalPathName& directory) {

    static const NoTocLocking none;

    if (!isOnNFS(locateExistingDirectory(directory))) {
        return none;
    }

    // Constructed on first use: an invalid policy throws here, and is reported again on the next call.
    static const NFSTocLocking nfs;

    return nfs;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
