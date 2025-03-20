/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
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

    FileCopy() : src_(""), dest_(""), sync_(false) {}

    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName,
             bool sync = false) :
        src_(srcPath / fileName), dest_(destPath / fileName), sync_(sync) {}

    FileCopy(eckit::Stream& s) {
        s >> src_;
        s >> dest_;
        s >> sync_;
    }

    void encode(eckit::Stream& s) const {
        s << src_;
        s << dest_;
        s << sync_;
    }

    bool sync() { return sync_; }

    void execute() {
        eckit::FileHandle src(src_);
        eckit::FileHandle dest(dest_);
        src.copyTo(dest);
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

    void print(std::ostream& s) const { s << "FileCopy(src=" << src_ << ",dest=" << dest_ << ",sync=" << sync_ << ")"; }

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
};

//----------------------------------------------------------------------------------------------------------------------

using MoveElement = FileCopy;

using MoveIterator = APIIterator<MoveElement>;

using MoveAggregateIterator = APIAggregateIterator<MoveElement>;

using MoveAsyncIterator = APIAsyncIterator<MoveElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
