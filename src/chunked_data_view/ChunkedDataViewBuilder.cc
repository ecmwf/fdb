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
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ViewPart.h"

#include "eckit/exception/Exceptions.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chunked_data_view {

ChunkedDataViewBuilder::ChunkedDataViewBuilder(std::unique_ptr<FdbInterface> fdb) : fdb_(std::move(fdb)) {}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::addPart(std::string marsRequestKeyValues,
                                                        std::vector<AxisDefinition> axes,
                                                        std::unique_ptr<Extractor> extractor) {
    parts_.emplace_back(std::move(marsRequestKeyValues), std::move(axes), std::move(extractor));
    return *this;
}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::extendOnAxis(size_t index) {
    extensionAxisIndex_ = index;
    return *this;
}

bool ChunkedDataViewBuilder::doPartsAlign(const std::vector<ViewPart>& viewParts) {
    const ViewPart& first = viewParts[0];
    bool extensible       = true;
    for (const ViewPart& viewPart : viewParts) {
        extensible &= first.extensibleWith(viewPart, extensionAxisIndex_.value_or(0));
    }
    return extensible;
}

std::unique_ptr<ChunkedDataView> ChunkedDataViewBuilder::build() {
    if (parts_.empty()) {
        throw eckit::UserError("User must add at least one part to the view.");
    }

    if (parts_.size() > 1 && extensionAxisIndex_.has_value() == false) {
        throw eckit::UserError("Must specify an extension axis if multiple parts are specified.");
    }

    std::vector<ViewPart> viewParts{};
    viewParts.reserve(parts_.size());

    std::shared_ptr<FdbInterface> fdb = std::move(fdb_);
    for (auto& [req, defs, ext] : parts_) {
        auto request = fdb5::FDBToolRequest::requestsFromString(req).at(0).request();
        viewParts.emplace_back(std::move(request), std::move(ext), fdb, defs);
    }

    if (!doPartsAlign(viewParts)) {
        throw eckit::UserError("Shape of all parts must be identical except for the extension axis index.");
    }

    return std::make_unique<ChunkedDataViewImpl>(std::move(viewParts), extensionAxisIndex_.value_or(0));
}

};  // namespace chunked_data_view
