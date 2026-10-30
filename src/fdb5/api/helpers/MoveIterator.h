// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @author Simon Smart
/// @date   November 2018

#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>

#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/log/Log.h"
#include "eckit/os/Stat.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/api/helpers/APIIterator.h"

/*
 * Define a standard object which can be used to iterate the results of a
 * wipe() call on an arbitrary FDB object
 */

namespace fdb5 {


// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class FileCopy : public eckit::ThreadPoolTask {

public:

    FileCopy() : src_(""), dest_(""), sync_(false), preserveAttributes_(false) {}

    /// @param preserveAttributes reproduce the source's mode and mtime on the destination instead
    ///        of letting the copy take the prevailing umask and the time of day. Set only for the
    ///        access timestamps: their mtime is their entire content, and their mode is what lets
    ///        a non-owner record a read. Everything else is content, and a newly written file
    ///        should follow the umask as any other would.
    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName,
             bool sync = false, bool preserveAttributes = false) :
        src_(srcPath / fileName),
        dest_(destPath / fileName),
        sync_(sync),
        preserveAttributes_(preserveAttributes) {}

    FileCopy(eckit::Stream& s) {
        s >> src_;
        s >> dest_;
        s >> sync_;
        s >> preserveAttributes_;
    }

    void encode(eckit::Stream& s) const {
        s << src_;
        s << dest_;
        s << sync_;
        s << preserveAttributes_;
    }

    bool sync() { return sync_; }

    void execute() {
        {
            eckit::FileHandle src(src_);
            eckit::FileHandle dest(dest_);
            src.copyTo(dest);
        }

        if (preserveAttributes_) {
            copyAttributes();
        }
    }

    void cleanup() {
        if (src_.isDir()) {
            src_.rmdir(false);
        }
        else {
            src_.unlink(false);
        }
    }

private:  // methods

    /// Reproduce the source's mode and mtime on the destination. Best-effort throughout: a move
    /// that has already copied the content is not failed because its metadata could not be
    /// carried across, so each step reports and continues.
    void copyAttributes() const {

        eckit::Stat::Struct info;
        if (::stat(src_.localPath(), &info) < 0) {
            eckit::Log::warning() << "Could not stat " << src_ << " to carry its attributes across: "
                                  << ::strerror(errno) << std::endl;
            return;
        }

        if (::chmod(dest_.localPath(), info.st_mode & 07777) < 0) {
            eckit::Log::warning() << "Could not set the mode of " << dest_ << ": " << ::strerror(errno) << std::endl;
        }

        struct ::timespec times[2];
        times[0].tv_sec = info.st_atime;
        times[0].tv_nsec = 0;
        times[1].tv_sec = info.st_mtime;
        times[1].tv_nsec = 0;
        if (::utimensat(AT_FDCWD, dest_.localPath(), times, 0) < 0) {
            eckit::Log::warning() << "Could not set the timestamps of " << dest_ << ": " << ::strerror(errno)
                                  << std::endl;
        }
    }

    void print(std::ostream& s) const {
        s << "FileCopy(src=" << src_ << ",dest=" << dest_ << ",sync=" << sync_
          << ",preserveAttributes=" << preserveAttributes_ << ")";
    }

    friend std::ostream& operator<<(std::ostream& s, const FileCopy& f) {
        f.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const FileCopy& f) {
        f.encode(s);
        return s;
    }

private:

    eckit::PathName src_;
    eckit::PathName dest_;
    bool sync_;
    bool preserveAttributes_;
};

//----------------------------------------------------------------------------------------------------------------------

using MoveElement = FileCopy;

using MoveIterator = APIIterator<MoveElement>;

using MoveAggregateIterator = APIAggregateIterator<MoveElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
