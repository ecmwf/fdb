/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "RequestManipulation.h"

#include "Axis.h"

#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/IndexMapper.h"
#include "chunked_data_view/exception/BoundingBoxException.h"
#include "chunked_data_view/exception/RequestManipulationException.h"
#include "chunked_data_view/mapping/AxisMapper.h"
#include "eckit/exception/Exceptions.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace chunked_data_view {

metkit::mars::MarsRequest RequestManipulation::selectRequest(const metkit::mars::MarsRequest& request,
                                                             const std::vector<Axis>& axes,
                                                             const PartBoundingBox& boundingBox) {

    if (boundingBox.dimensions() != axes.size()) {  // ParBoundingBox contains the field sizes as last entry
        std::stringstream buf;
        buf << "RequestManipulation::selectRequest: Bounding Box dimensions mismatch. Bounding Box must have axes "
               "dimensions - 1. But bounding box dimensions were "
            << boundingBox.dimensions() << " and axes dimensions were " << axes.size();
        throw chunked_data_view::BoundingBoxException(buf.str());
    }

    if (boundingBox.entries() == 0) {
        throw chunked_data_view::BoundingBoxException(
            "RequestManipulation::selectRequest: Bounding Box empty. This would result in an empty MARS request.");
    }

    // Add check for subaxis aligned bounding box


    // TODO(TKR): Get the subrequest for a bounding box by finding the inner part offset and
    // restricting the request accordingly.
    // Later on we can map for each key of a read message to the corresponding buffer index
    // Prop. adjust the update Request method to take an upper lower bound for the values of an axis.

    metkit::mars::MarsRequest result = request;

    for (size_t idx = 0; idx < boundingBox.dimensions(); ++idx) {
        RequestManipulation::updateRequest(result, axes[idx], boundingBox.lower()[idx], boundingBox.upper()[idx]);
    }
    return result;
}

std::vector<std::string> removeDuplicates(const std::vector<std::string>& myVector) {
    std::unordered_set<std::string> seen;

    std::vector<std::string> result = myVector;

    auto newEnd = remove_if(result.begin(), result.end(), [&seen](const std::string& value) {
        // Checking if value has been seen; if not, add
        // to seen and keep in vector
        if (seen.find(value) == seen.end()) {
            seen.insert(value);
            return false;  // Don't remove the item
        }
        return true;  // Remove the item
    });

    // Erase the non-unique elements
    result.erase(newEnd, result.end());

    return result;
}


void RequestManipulation::updateRequest(metkit::mars::MarsRequest& request, const Axis& axis, size_t lowerIndex,
                                        size_t upperIndex) {

    ASSERT(lowerIndex >= 0);
    ASSERT(lowerIndex <= upperIndex);
    ASSERT(upperIndex < axis.size());

    const auto dimCount = axis.parameters().size();
    std::unordered_map<std::string, std::vector<std::string>> new_params;

    for (size_t i = 0; i < dimCount; ++i) {
        const auto& parameter = axis.parameters()[i];
        new_params.emplace(parameter.name(), std::vector<std::string>{});
    }

    for (size_t index = lowerIndex; index <= upperIndex; index++) {
        const auto mappedIndex = index_mapping::to_axis_parameter_index(index, axis);

        // Add all parameter values to the request
        for (size_t i = 0; i < dimCount; ++i) {
            const auto& parameter = axis.parameters()[i];
            new_params[parameter.name()].emplace_back(parameter.values()[mappedIndex[i]]);
        }
    }

    // Chunk alignment
    size_t entries = 1;


    for (const auto& [name, values] : new_params) {
        // We would need multiple MARS requests if the amount of entries of the 'hypercube' in the MARS
        // request is more than the indices we asked for (after removing potential doubles)
        const auto removedDoubles = removeDuplicates(values);
        entries *= removedDoubles.size();
        request.values(name, removedDoubles);
    }

    if (entries > upperIndex - lowerIndex + 1) {
        std::stringstream buf;
        buf << "RequestManipulation::updateRequest: Bounding Box is not aligned with sub-axis of the merge axis. This "
               "would result in two needed MARS requests which is not supported.";
        throw chunked_data_view::RequestManipulationException(buf.str());
    }
}

}  // namespace chunked_data_view
