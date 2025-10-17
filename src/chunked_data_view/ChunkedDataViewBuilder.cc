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

#include "fdb5/api/helpers/FDBToolRequest.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chunked_data_view {

ChunkedDataViewBuilder::ChunkedDataViewBuilder(std::unique_ptr<Fdb> fdb) : fdb_(std::move(fdb)) {}

ChunkedDataViewBuilder& ChunkedDataViewBuilder::addPart(std::string marsRequestKeyValues,
                                                        std::vector<AxisDefinition> axes,
                                                        std::unique_ptr<Extractor> extractor) {
    parts_.emplace_back(std::move(marsRequestKeyValues), std::move(axes), std::move(extractor));
    return *this;
}

// TODO(kkratz): extensionAxisIndex has to become an optional that _NEEDS_ to be set if more than one part has been
// added because haveing multiple parts only makes sense if the extend on one axis.
ChunkedDataViewBuilder& ChunkedDataViewBuilder::extendOnAxis(size_t index) {
    extensionAxisIndex_ = index;
    return *this;
}

std::unique_ptr<ChunkedDataView> ChunkedDataViewBuilder::build() {
    std::vector<ViewPart> viewParts{};
    viewParts.reserve(parts_.size());
    std::shared_ptr<Fdb> fdb = std::move(fdb_);
    for (auto& [req, defs, ext] : parts_) {
        auto request = fdb5::FDBToolRequest::requestsFromString(req).at(0).request();
        viewParts.emplace_back(std::move(request), std::move(ext), fdb, defs);
    }
    // TODO(kkratz): Verfiy configuration:
    // - Ensure all requests define same Axis
    // - Exatly one axis must be an extension

    return std::make_unique<ChunkedDataViewImpl>(std::move(viewParts), extensionAxisIndex_);
}

};  // namespace chunked_data_view
