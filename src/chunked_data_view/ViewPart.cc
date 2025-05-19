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

namespace chunked_data_view {

ViewPart::ViewPart(metkit::mars::MarsRequest request, std::unique_ptr<Extractor> extractor, std::shared_ptr<Fdb> fdb,
                   const std::vector<AxisDefinition>& axes) :
    request_(std::move(request)), extractor_(std::move(extractor)), fdb_(std::move(fdb)) {
    ASSERT(fdb_);
    axes_.reserve(axes.size());

    std::set<std::string> processedKeywords{};
    for (const auto& axis : axes) {

        std::vector<Axis::Parameter> parameters{};
        parameters.reserve(axis.keys.size());
        for (const auto& key : axis.keys) {
            if (processedKeywords.count(key) != 0) {
                throw std::runtime_error("ViewPart::ViewPart:Keyword already mapped by another axis");
            }
            processedKeywords.insert(key);
            parameters.emplace_back(key, request_.values(key));
        }
        axes_.emplace_back(parameters, axis.chunked);
    }

    for (const auto& p : request_.parameters()) {
        if (p.count() > 1 && processedKeywords.count(p.name()) != 1) {
            // TODO(kkratz): Use appropiate exception
            std::stringstream buf{};
            buf << "ViewPart::ViewPart:Keyword " << p.name() << " has " << p.count()
                << " values but is not mapped by an axis.";
            throw std::runtime_error(buf.str());
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

void ViewPart::at(const std::vector<size_t>& chunkIndex, uint8_t* data, size_t size) const {
    ASSERT(chunkIndex.size() - 1 == axes_.size());
    auto request = request_;
    for (size_t idx = 0; idx < chunkIndex.size() - 1; ++idx) {
        axes_[idx].updateRequest(request, chunkIndex[idx]);
    }
    auto dh = fdb_->retrieve(request);
    extractor_->writeInto(*dh, data, layout_);
}

metkit::mars::MarsRequest ViewPart::requestAt(const std::vector<size_t>& chunkIndex) const {
    ASSERT(chunkIndex.size() == axes_.size());
    auto request = request_;
    for (size_t idx = 0; idx < chunkIndex.size(); ++idx) {
        axes_[idx].updateRequest(request, chunkIndex[idx]);
    }
    return request;
}

}  // namespace chunked_data_view
