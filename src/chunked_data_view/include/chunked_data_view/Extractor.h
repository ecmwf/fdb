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
    /// The caller must ensure there is enough memory allocated for all values to be copied into out.
    /// @param request that was used to read data from FDB, only used to enhance error messages
    /// @param list_iterator to read data from
    /// @param axes of the corresponding view.
    /// @param layout of the expected field.
    /// @param ptr pointer to write into.
    /// @param len of memory pointed to by ptr in floats
    /// @param extensionAxisIdx index of the extension axis (SIZE_MAX = no extension)
    /// @param combinedExtSize combined size of the extension axis across all parts
    /// @param extensionOffset offset of this part on the extension axis
    /// @return number of messages written
    virtual size_t writeInto(const metkit::mars::MarsRequest& request,
                             std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
                             const DataLayout& layout, float* ptr, size_t len, size_t extensionAxisIdx = SIZE_MAX,
                             size_t combinedExtSize = 0, size_t extensionOffset = 0) const = 0;
};

enum class ExtractorType {
    GRIB
};

std::unique_ptr<Extractor> makeExtractor(ExtractorType type);

}  // namespace chunked_data_view
