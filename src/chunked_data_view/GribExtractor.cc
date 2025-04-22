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

#include <eckit/message/Reader.h>

namespace chunked_data_view {
DataLayout GribExtractor::layout(eckit::DataHandle& handle) const {
    eckit::message::Reader reader(handle);
    eckit::message::Message msg = reader.next();
    if (!msg) {
        // TODO(kkratz) Use proper exception, use helpful message
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }
    size_t countValues = msg.getSize("values");
    return {countValues, 8};
}

void GribExtractor::writeInto(eckit::DataHandle& handle, uint8_t* out, const DataLayout& layout) const {
    // TODO(kkratz): Add error handling and length checks!
    eckit::message::Reader reader(handle);
    eckit::message::Message msg{};
    auto copyInto = reinterpret_cast<double*>(out);
    // TODO(kkratz): This just copies the date in the order as it is retrieved!
    while ((msg = reader.next())) {
        size_t countValues = msg.getSize("values");
        msg.getDoubleArray("values", copyInto, countValues);
        copyInto += countValues;
    }
}

};  // namespace chunked_data_view
