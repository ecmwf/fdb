/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "chunked_data_view/ChunkedDataViewBuilder.h"

#include "ChunkedDataViewImpl.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ChunkedDataView.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/mapping/AxisMapper.h"

#include "eckit/exception/Exceptions.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include <cassert>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace chunked_data_view {

ChunkedDataViewBuilder::ChunkedDataViewBuilder(const std::optional<std::filesystem::path>& fdbConfigPath) :
    configPath_(fdbConfigPath) {}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::addPart(std::string marsRequestKeyValues,
                                                        std::vector<AxisDefinition> axes,
                                                        std::shared_ptr<Extractor> extractor) {
    parts_.emplace_back(std::move(marsRequestKeyValues), std::move(axes), std::move(extractor));
    return *this;
}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::extendOnAxis(size_t index) {
    extensionAxisIndex_ = std::make_optional<size_t>(index);
    return *this;
}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::fillValue(float fillValue) {
    fillValue_ = fillValue;
    return *this;
}

bool ChunkedDataViewBuilder::doPartsAlign(
    const std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>>& viewParts) {
    const ViewPart& first = std::get<0>(viewParts[0]);
    bool extensible = true;
    for (const auto& [viewPart, _] : viewParts) {
        extensible &= first.extensibleWith(viewPart, extensionAxisIndex_.value_or(0));
    }
    return extensible;
}

std::unique_ptr<ChunkedDataView> ChunkedDataViewBuilder::build() {
    if (parts_.empty()) {
        throw eckit::UserError("ChunkedDataViewBuilder::build: User must add at least one part to the view.");
    }

    if (parts_.size() > 1 && extensionAxisIndex_.has_value() == false) {
        throw eckit::UserError(
            "ChunkedDataViewBuilder::build: Must specify an extension axis if multiple parts are specified.");
    }
    if (extensionAxisIndex_.has_value()) {
        const auto& firstPartAxis = std::get<1>(parts_[0]);
        if (extensionAxisIndex_.value() >= firstPartAxis.size()) {
            throw eckit::UserError("ChunkedDataViewBuilder::build: ExtensionAxis is not referring to a valid axis.");
        }
    }

    std::vector<std::pair<ViewPart, std::shared_ptr<Extractor>>> viewParts{};
    viewParts.reserve(parts_.size());

    // Offset is one-dimensional along the extension axis
    std::vector<size_t> part_offsets = {0};

    for (auto& [req, defs, ext] : parts_) {
        ext->setFillValue(fillValue_);

        auto request = fdb5::FDBToolRequest::requestsFromString(req).at(0).request();

        try {
            const auto layout = ext->layout(request);
            const auto axes = AxisMapper::mapRequestToAxis(request, defs);

            // Create offset vector
            std::vector<size_t> offsetInChunkedDataView(axes.size(), 0);
            offsetInChunkedDataView[extensionAxisIndex_.value_or(0)] = part_offsets[part_offsets.size() - 1];

            ViewPart vp(std::move(request), layout, axes, offsetInChunkedDataView);
            part_offsets.push_back(part_offsets.back() + vp.extension()[extensionAxisIndex_.value_or(0)]);

            viewParts.emplace_back(std::move(vp), std::move(ext));
        }
        catch (const std::exception& e) {
            std::ostringstream ss;
            ss << "Cannot create view, no data found for request " << req
               << " to establish field size. Underlying error: " << e.what();
            throw eckit::UserError(ss.str());
        }
    }

    if (!doPartsAlign(viewParts)) {
        throw eckit::UserError("Shape of all parts must be identical except for the extension axis index.");
    }

    return std::make_unique<ChunkedDataViewImpl>(viewParts, fillValue_, extensionAxisIndex_.value_or(0));
}

};  // namespace chunked_data_view
