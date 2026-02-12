/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "GribExtractor.h"

#include "chunked_data_view/DataLayout.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/message/Reader.h"

#include <cstddef>
#include <ostream>
#include <sstream>


namespace chunked_data_view {
DataLayout GribExtractor::layout(eckit::DataHandle& handle) const {
    eckit::message::Reader reader(handle);
    eckit::message::Message msg = reader.next();
    if (!msg) {
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }
    size_t countValues = msg.getSize("values");
    return {countValues, 4};
}

void GribExtractor::extract(eckit::DataHandle& handle, const DataLayout& layout, float* ptr, size_t len) const {
    ASSERT(len >= layout.countValues);

    eckit::message::Reader reader(handle);
    eckit::message::Message msg = reader.next();
    if (!msg) {
        throw eckit::Exception("GribExtractor::extract: No GRIB message in handle.");
    }

    if (const auto size = msg.getSize("values"); size != layout.countValues) {
        std::ostringstream ss;
        ss << "GribExtractor::extract: Unexpected field size. Expected: "
           << layout.countValues << ", found: " << size
           << ". All fields in a view must be of equal size.";
        throw eckit::Exception(ss.str());
    }

    msg.getFloatArray("values", ptr, layout.countValues);

    if (eckit::message::Message extra = reader.next(); extra) {
        throw eckit::Exception(
            "GribExtractor::extract: Expected a single GRIB message per field, but found more than one.");
    }
}
};  // namespace chunked_data_view
