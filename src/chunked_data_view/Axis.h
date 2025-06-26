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

#include <metkit/mars/MarsRequest.h>

#include <string>
#include <tuple>
#include <vector>

namespace chunked_data_view {

class Axis {
public:

    using Parameter = std::tuple<const std::string, const std::vector<std::string>>;

    Axis(std::vector<Parameter> parameters, bool chunked);

    /// Updates MarsRequest parameters to match the requested chunk index.
    /// @param[in,out] request to modify
    /// @param chunkIndex to compute parameter values from
    void updateRequest(metkit::mars::MarsRequest& request, size_t chunkIndex) const;

    size_t size() const { return size_; }

    bool isChunked() const { return chunked_; }
    const std::vector<Parameter>& parameters() const { return parameters_; }

private:

    std::vector<Parameter> parameters_{};
    size_t size_{};
    bool chunked_{};
};

}  // namespace chunked_data_view
