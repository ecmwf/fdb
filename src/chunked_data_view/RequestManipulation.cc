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

#include "eckit/exception/Exceptions.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <vector>

namespace chunked_data_view {

void RequestManipulation::updateRequest(metkit::mars::MarsRequest& request, const Axis& axis, size_t chunkIndex) {
    if (!axis.isChunked()) {
        ASSERT(chunkIndex == 0);
        for (const auto& parameter : axis.parameters()) {
            request.values(parameter.name(), parameter.values());
        }
        return;
    }
    ASSERT(chunkIndex < axis.size());

    const auto dimCount = axis.parameters().size();
    std::vector<size_t> index(dimCount, 0);

    size_t chunk_index = chunkIndex;

    // Idea: All dimension on the right side of the current index are multiplied and subtracted
    // from the current index. The division in (1) computes the current index. (2) computes the
    // remainder and afterwards we continue with the next index.
    for (size_t i = 0; i < dimCount - 1; ++i) {
        size_t dim_prod = 1;

        for (size_t j = i + 1; j < dimCount; ++j) {
            dim_prod *= axis.parameters()[j].values().size();
        }

        size_t index_in_dim = chunk_index / dim_prod;  // (1)
        index[i] = index_in_dim;
        chunk_index -= index_in_dim * dim_prod;  // (2)
    }

    // Compute the final remainder
    index[dimCount - 1] = chunk_index % axis.parameters()[dimCount - 1].values().size();

    // Add all parameter values to the request
    for (size_t i = 0; i < dimCount; ++i) {
        const auto& parameter = axis.parameters()[i];
        request.values(parameter.name(), {parameter.values()[index[i]]});
    }
}

}  // namespace chunked_data_view
