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

#include "chunked_data_view/DataLayout.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <vector>

namespace eckit {
class DataHandle;
}

namespace chunked_data_view {
class ViewPart;
}

namespace chunked_data_view {

class Extractor {
public:

    virtual ~Extractor() = default;

    /// Only first message will be read
    /// @param handle to a stream of grib messages
    /// @return the data
    virtual DataLayout layout(const metkit::mars::MarsRequest& req) const = 0;

    /// Writes the extracted data into the out pointer.
    /// The caller must ensure there is enough memory allocated for all values to be copied into out.
    /// @param list_iterator to read data from
    /// @param axes of the corresponding view.
    /// @param layout of the expected field.
    /// @param ptr pointer to write into.
    /// @param len of memory pointed to by ptr in floats
    /// @param extensionAxisIdx index of the extension axis (SIZE_MAX = no extension)
    /// @param combinedExtSize combined size of the extension axis across all parts
    /// @param extensionOffset offset of this part on the extension axis
    /// @return number of messages written
    // virtual size_t writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
    //                          const DataLayout& layout, float* ptr, size_t len, size_t extensionAxisIdx = SIZE_MAX,
    //                          size_t combinedExtSize = 0, size_t extensionOffset = 0) const = 0;

    virtual size_t extractInto(const ViewPart& part, const std::vector<std::size_t>& chunkIndex, float* ptr, size_t len,
                               size_t extensionAxisIdx = SIZE_MAX, size_t combinedExtSize = 0,
                               size_t extensionOffset = 0) const = 0;
};

enum class ExtractorType {
    GRIB
};
}  // namespace chunked_data_view
