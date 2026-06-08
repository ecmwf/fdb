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

#include "Axis.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <vector>

namespace chunked_data_view {

class ViewPart {
public:

    ViewPart(const metkit::mars::MarsRequest& request, const DataLayout& data_layout,
             const std::vector<AxisDefinition>& axes);
    metkit::mars::MarsRequest at(const std::vector<size_t>& chunkIndex) const;
    std::vector<size_t> shape() const { return shape_; }
    const DataLayout& layout() const { return layout_; }
    const std::vector<Axis>& axes() const { return axes_; }

    bool isAxisChunked(size_t index) const { return axes_.at(index).isChunked(); };

    bool extensibleWith(const ViewPart& other, size_t extension_axis) const;

private:

    metkit::mars::MarsRequest requestAt(const std::vector<size_t>& chunkIndex) const;

    // Each keyword defines a potential axis in the resulting view.
    // No axis needs to be created if the cardinality is one.
    // Each keyword with cardinality greater than 1 needs to be covered by exactly one
    // axis definition
    metkit::mars::MarsRequest request_{};
    std::vector<Axis> axes_{};
    // std::shared_ptr<Extractor> extractor_{};
    DataLayout layout_{};
    std::vector<size_t> shape_{};
};

}  // namespace chunked_data_view
