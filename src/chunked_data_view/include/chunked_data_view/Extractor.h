/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/ListIterator.h"

#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace eckit {
class DataHandle;
}

namespace chunked_data_view {

class Extractor {
public:

    virtual ~Extractor() = default;

    /// Only first message will be read
    /// @param handle to a stream of grib messages
    /// @return the data
    virtual DataLayout layout(eckit::DataHandle& handle) const = 0;

    /// Writes the extracted data into the out pointer.
    /// The caller must ensure there is enought memory allccated for all values to be copied into out.
    /// @param request that was used to read data from FDB, only used to enhance error messages
    /// @param list_iterator to read data from
    /// @param axes of the corresponding view.
    /// @param layout of the expected field.
    /// @param out pointer to write into.
    /// @param out len of memory pointed to by ptr in floats
    /// @param expected_msg_count number of GRIB messages expected
    virtual void writeInto(const metkit::mars::MarsRequest& request,
                           std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
                           const DataLayout& layout, float* ptr, size_t len, size_t expected_msg_count) const = 0;
    virtual void writeInto(eckit::DataHandle& dataHandle, float* datumPtr, size_t datumLen) const {};
};

enum class ExtractorType {
    GRIB
};

std::unique_ptr<Extractor> makeExtractor(ExtractorType type);

}  // namespace chunked_data_view
