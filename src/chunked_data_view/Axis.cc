/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Axis.h"

#include <numeric>
#include <utility>

namespace chunked_data_view {

namespace {
template <typename Params>
size_t combinedSize(const Params& params) {
    return std::accumulate(std::begin(params), std::end(params), 1,
                           [](auto acc, const auto& p) { return acc * std::get<1>(p).size(); });
}
}  // namespace

//======================================================================================================================
// class Axis
//======================================================================================================================

Axis::Axis(std::vector<Parameter> parameters, bool chunked) :
    parameters_(std::move(parameters)), size_(combinedSize(parameters_)), chunked_(chunked) {}

void Axis::updateRequest(metkit::mars::MarsRequest& request, size_t chunkIndex) const {
    if (!chunked_) {
        ASSERT(chunkIndex == 0);
        for (const auto& [key, values] : parameters_) {
            request.values(key, values);
        }
        return;
    }
    ASSERT(chunkIndex < size_);
    const auto dimCount = parameters_.size();
    static std::vector<size_t> index(dimCount);
    for (size_t i = 0; i < dimCount; ++i) {
        const size_t dim = std::get<1>(parameters_[dimCount - 1 - i]).size();
        index[i]         = chunkIndex % dim;
        chunkIndex /= dim;
    }

    auto indexRemainder = chunkIndex;
    for (auto iter = parameters_.rbegin(); iter != parameters_.rend(); ++iter) {
        const auto& [key, values] = *iter;
        const auto dim            = values.size();
        const auto dimIndex       = indexRemainder % dim;
        indexRemainder /= dim;
        request.values(key, {values[dimIndex]});
    }
}

}  // namespace chunked_data_view
