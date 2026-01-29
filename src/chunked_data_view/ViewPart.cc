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

#include "Axis.h"
#include "RequestManipulation.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>


namespace chunked_data_view {

ViewPart::ViewPart(metkit::mars::MarsRequest request, std::unique_ptr<Extractor> extractor,
                   std::shared_ptr<FdbInterface> fdb, const std::vector<AxisDefinition>& axes,
                   size_t extensionAxisOffset, size_t extensionAxisIndex) :
    request_(std::move(request)), extractor_(std::move(extractor)), fdb_(std::move(fdb)) {
    ASSERT(fdb_);

    // FIXME(kkratz): This transforms Axis Definitions + a MARS request into Axis -> Refactor to function
    // --BEGIN--
    axes_.reserve(axes.size());
    std::set<std::string> processedKeywords{};
    for (const auto& axis : axes) {

        std::vector<Parameter> parameters{};
        parameters.reserve(axis.keys.size());
        for (const auto& key : axis.keys) {
            if (processedKeywords.count(key) != 0) {
                throw eckit::UserError("ViewPart::ViewPart:Keyword already mapped by another axis");
            }
            processedKeywords.insert(key);
            parameters.emplace_back(std::make_tuple(key, request_.values(key)));
        }
        axes_.emplace_back(parameters, axis.chunked);
    }
    for (const auto& p : request_.parameters()) {
        if (p.count() > 1 && processedKeywords.count(p.name()) != 1) {
            std::ostringstream ss;
            ss << "ViewPart::ViewPart:Keyword " << p.name() << " has " << p.count()
               << " values but is not mapped by an axis.";
            throw eckit::UserError(ss.str());
        }
    }
    // --END--

    // TODO(kkratz): throw excpetion with contextual information, right now the error is not understandable
    const auto req = requestAt(ChunkIndex(axes_.size()));
    std::unique_ptr<eckit::DataHandle> result(fdb_->retrieve(req));
    layout_ = extractor_->layout(*result);

    GlobalIndex begin(axes_.size());
    GlobalIndex end(axes_.size());
    for (size_t axisIndex = 0; axisIndex < axes_.size(); ++axisIndex) {
        if (axisIndex == extensionAxisIndex) {
            begin[axisIndex] = extensionAxisOffset;
            end[axisIndex]   = extensionAxisOffset + axes_[axisIndex].size();
        }
        else {
            end[axisIndex] = axes_[axisIndex].size();
        }
    }
    region_ = GlobalRegion(begin, end);
}


void ViewPart::at(const GlobalRegion& requested_region, float* ptr, size_t len) const {
    ASSERT(requested_region.ndim() - 1 == axes_.size());
    const auto region = to_local_clipped(requested_region, region_);
    if (!region) {
        return;
    }

    auto request = request_;
    for (size_t idx = 0; idx < index.ndim() - 1; ++idx) {
        RequestManipulation::updateRequest(request, axes_[idx], index[idx]);
    }

    auto listIterator = fdb_->inspect(request);
    auto res          = listIterator->next();
    if (!res) {
        std::ostringstream ss;
        ss << "Empty iterator for request: " << request << ". Is the request correctly specified?";
        throw eckit::Exception(ss.str());
    }

    do {
        const auto& key       = std::get<0>(*res);
        auto& dataHandle      = std::get<1>(*res);
        const size_t msgIndex = computeBufferIndex(axes, key);

        extractor_->writeInto2(dataHandle, datumPtr, datumLen);
    } while ((res = listIterator->next()));

    // extractor_->writeInto(request, std::move(listIterator), axes_, layout_, ptr, len, extensionAxisOffset,
    //                       extensionAxisIndex_);
}

bool ViewPart::extensibleWith(const ViewPart& other, const size_t extension_axis) const {

    if (other.shape().ndim() != this->shape().ndim()) {
        return false;
    }

    for (size_t index = 0; index < other.shape().ndim(); ++index) {
        if (index == extension_axis) {
            continue;
        }
        // Checking the size of the combined axis
        // @info This is not checking whether the parameters are matching. We intentionally
        // can stitch mismatching parameters.
        if (other.shape()[index] != shape()[index]) {
            return false;
        }
    }

    return true;
}

metkit::mars::MarsRequest ViewPart::requestAt(const ChunkIndex& index) const {
    ASSERT(index.ndim() == axes_.size());
    auto request = request_;
    for (size_t idx = 0; idx < index.ndim(); ++idx) {
        RequestManipulation::updateRequest(request, axes_[idx], index[idx]);
    }
    return request;
}

bool ViewPart::hasData(const ChunkIndex& index) const {
    const auto& ax = axes_[extensionAxisIndex_];
    if (ax.isChunked()) {
        const auto idx = index[extensionAxisIndex_];
        // Requested index within index range provided by this part?
        return idx >= extensionAxisOffset_ && idx < extensionAxisOffset_ + ax.size();
    }
    // The extension axis is not chunked, this means we need the data from all view parts.
    return true;
}

}  // namespace chunked_data_view
