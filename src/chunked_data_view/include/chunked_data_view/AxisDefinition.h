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

#include <string>
#include <vector>

namespace chunked_data_view {

/// Describes how a axis in the resulting N Dimension data is build.
struct AxisDefinition {
    /// Which mars keys form the resuling axis.
    std::vector<std::string> keys{};
    /// If the axis is chunked all vallues can be indexed individualy,
    /// otherwise
    bool chunked{};
};

}  // namespace chunked_data_view
