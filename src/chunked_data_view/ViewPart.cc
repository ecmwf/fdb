/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "ViewPart.h"

#include "RequestManipulation.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/exception/GribExtractorException.h"

#include "chunked_data_view/mapping/AxisMapper.h"
#include "eckit/exception/Exceptions.h"
#include "metkit/mars/MarsRequest.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <set>
#include <string>
#include <vector>


namespace chunked_data_view {

ViewPart::ViewPart(const metkit::mars::MarsRequest& request, const DataLayout& data_layout,
                   const std::vector<AxisDefinition>& axes) :
    request_(request), layout_(data_layout) {
    axes_ = AxisMapper::mapRequestToAxis(request, axes);

    const auto req = requestAt(std::vector<size_t>(axes_.size()));
    extension_.reserve(axes_.size() + 1);
    std::transform(std::begin(axes_), std::end(axes_), std::back_inserter(extension_),
                   [](const auto& axis) { return axis.size(); });
    extension_.push_back(data_layout.countValues);
}


metkit::mars::MarsRequest ViewPart::at(const std::vector<size_t>& chunkIndex) const {
    ASSERT(chunkIndex.size() - 1 == axes_.size());
    return RequestManipulation::selectRequest(request_, axes_, chunkIndex);
}

metkit::mars::MarsRequest ViewPart::requestAt(const std::vector<size_t>& chunkIndex) const {
    ASSERT(chunkIndex.size() == axes_.size());
    auto request = request_;
    for (size_t idx = 0; idx < chunkIndex.size(); ++idx) {
        RequestManipulation::updateRequest(request, axes_[idx], chunkIndex[idx]);
    }
    return request;
}

bool ViewPart::extensibleWith(const ViewPart& other, const size_t extension_axis) const {

    if (other.extension().size() != this->extension().size()) {
        return false;
    }

    for (size_t index = 0; index < other.extension().size(); ++index) {
        if (index == extension_axis) {
            continue;
        }
        // Checking the size of the combined axis
        // @info This is not checking whether the parameters are matching. We intentionally
        // can stitch mismatching parameters.
        if (other.extension()[index] != extension()[index]) {
            return false;
        }
    }

    return true;
}

}  // namespace chunked_data_view
