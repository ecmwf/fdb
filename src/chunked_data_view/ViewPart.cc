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
                   std::shared_ptr<FdbInterface> fdb, const std::vector<AxisDefinition>& axes) :
    request_(std::move(request)), extractor_(std::move(extractor)), fdb_(std::move(fdb)) {
    ASSERT(fdb_);
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

    const auto req = requestAt(std::vector<size_t>(axes_.size()));
    std::unique_ptr<eckit::DataHandle> result(fdb_->retrieve(req));
    layout_ = extractor_->layout(*result);
    shape_.reserve(axes_.size() + 1);
    std::transform(std::begin(axes_), std::end(axes_), std::back_inserter(shape_),
                   [](const auto& axis) { return axis.size(); });
    shape_.push_back(layout_.countValues);
}


void ViewPart::at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len, size_t expected_msg_count) const {
    ASSERT(chunkIndex.size() - 1 == axes_.size());
    auto request = request_;
    for (size_t idx = 0; idx < chunkIndex.size() - 1; ++idx) {
        RequestManipulation::updateRequest(request, axes_[idx], chunkIndex[idx]);
    }
    auto listIterator = fdb_->inspect(request);
    extractor_->writeInto(request, std::move(listIterator), axes_, layout_, ptr, len, expected_msg_count);
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

    if (other.shape().size() != this->shape().size()) {
        return false;
    }

    for (size_t index = 0; index < other.shape().size(); ++index) {
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

}  // namespace chunked_data_view
