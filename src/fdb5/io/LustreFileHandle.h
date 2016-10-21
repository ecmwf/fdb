/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AdoptVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   October 2016

#ifndef fdb5_LustreFileHandle_h
#define fdb5_LustreFileHandle_h

#include "mars_server_config.h"

#if defined(LUSTREAPI_FOUND)
#include <lustre/lustreapi.h>
#else
#error Lustre API not found so LustreFileHandle is not supported
#endif

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

template< class HANDLE >
class LustreFileHandle : public HANDLE {
public:

    // -- Contructors

    LustreFileHandle(const std::string& path) :
        HANDLE(name),
        path(){
    }

    // -- Destructor

    virtual ~LustreFileHandle() {}

    virtual void openForAppend(const eckit::Length& len) {

        static unsigned int lustreStripeCount = eckit::Resource<unsigned int>("lustreStripeCount;$FDB5_LUSTRE_STRIPE_COUNT", 8);
        static size_t lustreStripeSize = eckit::Resource<size_t>("lustreStripeSize;$FDB5_LUSTRE_STRIPE_SIZE", 8*1024*1024);

        if(lustreStripeCount > 1) {

            /* From the docs: llapi_file_create closes the file descriptor. You must re-open the file afterwards */

            std::string path = path_;
            int rc = llapi_file_create(path_.c_str(), lustreStripeSize, 0, lustreStripeCount, LOV_PATTERN_RAID0);

            if(rc == EINVAL) {

                std::ostringstream oss;
                oss << "Invalid stripe parameters for Lustre file system"
                    << " - stripe count " << lustreStripeCount
                    << " - stripe size "  << lustreStripeSize;

                throw eckit::BadParameter(oss.str(), Here());
            }

            if(rc && rc != EEXIST) {
                throw eckit::FailedSystemCall("llapi_file_create", Here());
            }

        }

        this->HANDLE::openForAppend(len);

        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
