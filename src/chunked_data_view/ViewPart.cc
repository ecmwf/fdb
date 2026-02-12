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
#include "IndexMapper.h"
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
        // NOTE(kkratz): Extend here for configurable number of fields per chunk
        axes_.emplace_back(parameters, std::holds_alternative<AxisDefinition::IndividualChunking>(axis.chunking));
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
    try {
        std::unique_ptr<eckit::DataHandle> result(fdb_->retrieve(req));
        layout_ = extractor_->layout(*result);
    } catch (const std::exception& e) {
        std::ostringstream ss;
        ss << "Cannot create view, no data found for request " << req
           << " to establish field size. Underlying error: " << e.what();
        throw eckit::UserError(ss.str());
    }
    shape_.reserve(axes_.size() + 1);
    std::transform(std::begin(axes_), std::end(axes_), std::back_inserter(shape_),
                   [](const auto& axis) { return axis.size(); });
    shape_.push_back(layout_.countValues);
}


static size_t computeBufferIndex(const std::vector<Axis>& axes, const fdb5::Key& key) {
    std::vector<size_t> result;
    result.reserve(axes.size());

    for (const Axis& axis : axes) {
        if (axis.isChunked()) {
            result.push_back(0);
            continue;
        }
        result.emplace_back(axis.index(key));
    }

    ASSERT(result.size() == axes.size());
    return index_mapping::axis_index_to_buffer_index(result, axes);
}

void ViewPart::at(const std::vector<size_t>& chunkIndex, float* ptr, size_t len, size_t expected_msg_count) const {
    ASSERT(chunkIndex.size() - 1 == axes_.size());

    auto request = request_;
    for (size_t idx = 0; idx < chunkIndex.size() - 1; ++idx) {
        RequestManipulation::updateRequest(request, axes_[idx], chunkIndex[idx]);
    }

    auto listIterator = fdb_->inspect(request);
    bool iterator_empty = true;
    std::vector<bool> bitset(expected_msg_count);

    while (auto res = listIterator->next()) {
        if (!res) {
            break;
        }
        iterator_empty = false;

        const auto& key = std::get<0>(*res);
        auto& data_handle = std::get<1>(*res);

        const size_t msgIndex = computeBufferIndex(axes_, key);
        float* dest = ptr + msgIndex * layout_.countValues;
        ASSERT(dest + layout_.countValues - ptr <= static_cast<std::ptrdiff_t>(len));

        extractor_->extract(*data_handle, layout_, dest, layout_.countValues);
        bitset[msgIndex] = true;
    }

    if (iterator_empty) {
        std::ostringstream ss;
        ss << "ViewPart::at: Empty iterator for request: " << request
           << ". Is the request correctly specified?";
        throw eckit::Exception(ss.str());
    }

    if (const auto read_count = std::count(std::begin(bitset), std::end(bitset), true);
        read_count != static_cast<long>(bitset.size())) {
        std::ostringstream ss;
        ss << "ViewPart::at: Retrieved only " << read_count << " of " << bitset.size()
           << " fields in request " << request;
        throw eckit::Exception(ss.str());
    }
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
