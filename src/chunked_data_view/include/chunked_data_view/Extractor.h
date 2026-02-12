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

#include "eckit/io/DataHandle.h"

#include <cstddef>
#include <memory>

namespace eckit {
class DataHandle;
}

namespace chunked_data_view {

class Extractor {
public:

    virtual ~Extractor() = default;

    /// Only first message will be read
    /// @param handle to a stream of grib messages
    /// @return the data layout (field size and bytes per value)
    virtual DataLayout layout(eckit::DataHandle& handle) const = 0;

    /// Extract one field's values from a DataHandle into ptr.
    /// The caller is responsible for computing the correct buffer offset.
    /// @param handle to a single field's data
    /// @param layout expected field layout
    /// @param ptr destination buffer (already offset by caller)
    /// @param len available buffer size in floats
    virtual void extract(eckit::DataHandle& handle, const DataLayout& layout, float* ptr, size_t len) const = 0;
};

enum class ExtractorType {
    GRIB
};

std::unique_ptr<Extractor> makeExtractor(ExtractorType type);

}  // namespace chunked_data_view
