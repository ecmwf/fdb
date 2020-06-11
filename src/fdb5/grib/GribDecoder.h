/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   GribDecoder.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_GribDecoder_H
#define fdb5_GribDecoder_H


#include "eckit/io/Buffer.h"
#include "metkit/mars/MarsRequest.h"
#include <vector>

struct grib_handle;

namespace eckit {
class PathName;
}

namespace metkit { namespace grib { class MetFile; }}

#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class GribDecoder {
public:

    GribDecoder(bool checkDuplicates = false);

    virtual ~GribDecoder();

    size_t gribToKey(metkit::grib::MetFile& file, Key &key);
    metkit::mars::MarsRequest gribToRequest(const eckit::PathName &path, const char *verb = "retrieve");
    std::vector<metkit::mars::MarsRequest> gribToRequests(const eckit::PathName &path, const char *verb = "retrieve");

protected:

    const eckit::Buffer &buffer() const {
        return buffer_;
    }

private:

    virtual void patch(grib_handle*);

    eckit::Buffer buffer_;
    std::set<Key> seen_;
    bool checkDuplicates_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
