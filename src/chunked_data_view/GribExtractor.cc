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

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/IndexMapper.h"
#include "chunked_data_view/ListIterator.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/message/Reader.h"
#include "fdb5/database/Key.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <ostream>
#include <sstream>
#include <vector>


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

size_t computeBufferIndex(const std::vector<Axis>& axes, const fdb5::Key& key) {
    std::vector<size_t> result;
    result.reserve(axes.size());

    for (const Axis& axis : axes) {

        if (axis.isChunked()) {
            result.push_back(0);
            continue;
        }

        result.emplace_back(axis.index(key));
    }

    ASSERT(result.size() == axes.size());

    return chunked_data_view::index_mapping::axis_index_to_buffer_index(result, axes);
}

void GribExtractor::writeInto2(eckit::DataHandle& dataHandle, float* datumPtr, size_t datumLen) const {
    eckit::message::Reader reader(dataHandle);
    eckit::message::Message msg{};

    msg = reader.next();
    if (const auto size = msg.getSize("values"); size != datumLen) {
        std::ostringstream ss;
        // TODO(kkratz): need to fix error message, key information missing here.
        ss << "GribExractor: Unexpected field size found in GRIB message for key: " << ""
           << " expected: " << datumLen << " found: " << size << ". All fields in your view need to be of equal size.";
        throw eckit::Exception(ss.str());
    }
    msg.getFloatArray("values", datumPtr, datumLen);

    msg = reader.next();
    if (msg) {
        std::ostringstream ss;
        ss << "GribExtractor: One message per DataHandle expected!";
        // TODO(kkratz): This should be a fatal error
        throw eckit::Exception(ss.str());
    }
}
};  // namespace chunked_data_view
