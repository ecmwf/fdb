/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocCommon.h
/// @author Emanuele Danovaro
/// @date   August 2019

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

class TocCommon {
public:

    TocCommon(const eckit::PathName& path);
    virtual ~TocCommon() {}

    static eckit::PathName findRealPath(const eckit::PathName& path);
    static std::string userName(uid_t uid);

    virtual void checkUID() const;

    virtual const eckit::PathName& basePath() const { return directory_; }

    std::string owner() const { return userName(dbUID()); }

protected: // methods

    virtual uid_t dbUID() const;

protected: // members

    const eckit::PathName directory_;
    const eckit::PathName schemaPath_;

    mutable uid_t dbUID_;
    uid_t userUID_;

    mutable bool dirty_;
};

// class for writing a chunk of the user buffer - used to perform multiple simultaneous writes
class FileCopy : public eckit::ThreadPoolTask {
    eckit::PathName src_;
    eckit::PathName dest_;

public:
    FileCopy(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName):
        src_(srcPath / fileName), dest_(destPath / fileName) {}
    FileCopy(char* files, int size) {
        std::string src, dest, file;
        int i=0;

        for (; i<size && files[i] != '\0'; i++) {
            src += files[i];
        }
        src_ = src;
        i++;
        for (; i<size && files[i] != '\0'; i++) {
            dest += files[i];
        }
        dest_ = dest;
    }

    void str(char bufr[], int size) {
        int l1 = src_.asString().length();
        ::strncpy(bufr, src_.asString().c_str(), size);
        bufr[l1] = '\0';
        int l2 = dest_.asString().length();
        ::strncpy(&(bufr[l1+1]), dest_.asString().c_str(), size-l1-1);
        bufr[l1+l2+1] = '\0';
    }

    void execute() {
        eckit::FileHandle src(src_);
        eckit::FileHandle dest(dest_);
        src.copyTo(dest);
    }
};

}
