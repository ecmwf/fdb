/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   LustreFileHandle.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   October 2016

#ifndef fdb5_io_LustreFileHandle_h
#define fdb5_io_LustreFileHandle_h

#include "fdb5/fdb5_config.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/log/Bytes.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/io/LustreSettings.h"

namespace fdb5 {

template <class HANDLE>
class LustreFileHandle : public HANDLE {

public:  // methods

    LustreFileHandle(const std::string& path, LustreStripe stripe) : HANDLE(path), stripe_(stripe) {}

    LustreFileHandle(const std::string& path, size_t buffsize, LustreStripe stripe) :
        HANDLE(path, buffsize), stripe_(stripe) {}

    LustreFileHandle(const std::string& path, size_t buffcount, size_t buffsize, LustreStripe stripe) :
        HANDLE(path, buffcount, buffsize), stripe_(stripe) {}

    ~LustreFileHandle() override {}

    void openForAppend(const eckit::Length& len) override {

        std::string pathStr = HANDLE::path_;
        eckit::PathName path{pathStr};

        if (path.exists())
            return;  //< Lustre API outputs ioctl error messages when called on files exist

        /* From the docs: llapi_file_create closes the file descriptor. You must re-open the file afterwards */

        LOG_DEBUG_LIB(LibFdb5) << "Creating Lustre file " << pathStr << " with " << stripe_.count_ << " stripes "
                               << "of " << eckit::Bytes(stripe_.size_) << std::endl;

        int err = fdb5LustreapiFileCreate(path, stripe_);

        if (err == EINVAL) {

            std::ostringstream oss;
            oss << "Invalid stripe parameters for Lustre file system"
                << " - stripe count " << stripe_.count_ << " - stripe size " << stripe_.size_;

            throw eckit::BadParameter(oss.str(), Here());
        }

        if (err && err != EEXIST && err != EALREADY) {
            throw eckit::FailedSystemCall("llapi_file_create", Here());
        }

        this->HANDLE::openForAppend(len);
    }

private:  // members

    LustreStripe stripe_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
