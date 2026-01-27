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
#include "chunked_data_view/Fdb.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace chunked_data_view {

class ViewPart {
public:

    ViewPart(metkit::mars::MarsRequest request, std::unique_ptr<Extractor> extractor, std::shared_ptr<FdbInterface> fdb,
             const std::vector<AxisDefinition>& axes, size_t extensionAxisOffset, size_t extensionAxisIndex);
    void at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len) const;
    std::vector<size_t> shape() const { return shape_; }
    const DataLayout& layout() const { return layout_; }
    bool isAxisChunked(size_t index) { return axes_.at(index).isChunked(); };

    bool extensibleWith(const ViewPart& other, size_t extension_axis) const;

private:

    metkit::mars::MarsRequest requestAt(const std::vector<size_t>& chunkIndex) const;

    bool hasData(const std::vector<size_t>& chunkIndex) const;

    // Each keyword defines a potential axis in the resulting view.
    // No axis needs to be created if the cardinality is one.
    // Each keyword with cardinality greater than 1 needs to be covered by exactly one
    // axis definition
    metkit::mars::MarsRequest request_{};
    std::vector<Axis> axes_{};
    std::unique_ptr<Extractor> extractor_{};
    std::shared_ptr<FdbInterface> fdb_{};
    DataLayout layout_{};
    std::vector<size_t> shape_{};
    /// Offset of this part in the extension axis.
    size_t extensionAxisOffset_{};
    size_t extensionAxisIndex_{};
};

}  // namespace chunked_data_view
