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

    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName, bool sync=false) :
        src_(srcPath / fileName), dest_(destPath / fileName), sync_(sync) {}

    explicit FileCopy(eckit::Stream& stream) {
        stream >> src_;
        stream >> dest_;
        stream >> sync_;
    }

    void encode(eckit::Stream& stream) const {
        stream << src_;
        stream << dest_;
        stream << sync_;
    } 

    bool sync() const { return sync_; }

    void execute() override {
        eckit::FileHandle src(src_);
        eckit::FileHandle dest(dest_);
        src.copyTo(dest);
    }

    void cleanup() {
        if (src_.isDir()) {
            src_.rmdir(false);
        } else {
            src_.unlink(false);
        }
    }

private: // methods

    void print(std::ostream& ostream) const {
        ostream << "FileCopy(src=" << src_ << ",dest=" << dest_ << ",sync=" << sync_ << ")";
    }

    friend std::ostream& operator<<(std::ostream& ostream, const FileCopy& fileCopy) {
        fileCopy.print(ostream);
        return ostream;
    }

    friend eckit::Stream& operator<<(eckit::Stream& stream, const FileCopy& fileCopy) {
        fileCopy.encode(stream);
        return stream;
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

} // namespace fdb5
