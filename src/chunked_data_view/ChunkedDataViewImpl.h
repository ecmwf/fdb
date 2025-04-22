/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once

#include "ViewPart.h"
#include "chunked_data_view/ChunkedDataView.h"

#include <eckit/io/DataHandle.h>
#include <fdb5/api/helpers/FDBToolRequest.h>
#include <metkit/codes/GribHandle.h>
#include <metkit/mars/MarsRequest.h>

#include <vector>


namespace chunked_data_view {

class ChunkedDataViewImpl : public ChunkedDataView {
public:

    ChunkedDataViewImpl(std::vector<ViewPart> partialViews, size_t extensionAxisIndex);

    /// @param index n-dim chunk index
    const std::vector<double>& at(const std::vector<size_t>& chunkIndex) override;
    size_t size() const override { return data_.size() * sizeof(decltype(data_)::value_type); };
    const std::vector<size_t>& chunkShape() const override { return chunkShape_; }
    const std::vector<size_t>& shape() const override { return shape_; }

private:

    std::vector<size_t> chunkShape_{};
    std::vector<size_t> shape_{};
    std::vector<ViewPart> parts_{};
    size_t extensionAxisIndex_{};
    std::vector<double> data_{};
};

}  // namespace chunked_data_view
