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
#include "chunked_data_view/Extractor.h"

#include <cstddef>

namespace chunked_data_view {
class GribExtractor final : public Extractor {
public:

    DataLayout layout(eckit::DataHandle& handle) const override;

    void extract(eckit::DataHandle& handle, const DataLayout& layout, float* ptr, size_t len) const override;
};
}  // namespace chunked_data_view
