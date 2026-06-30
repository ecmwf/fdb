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
#include <variant>
#include <vector>

namespace chunked_data_view {

/// Describes how a axis in the resulting N Dimension data is build.
struct AxisDefinition {
    // NOTE(kkratz): Extend here for configurable number of values per chunk
    // ChunkingType is designed as sum-type so that we can extend this with a type holding
    // the number of values per chunk. Right now its just all in one chunk or one per chunk.

    /// Indicate no chunking should be applied, the whole axis is accessed in one chunk.
    struct NoChunking {};
    /// Indicate each value of this axis is one chunk
    struct IndividualChunking {};
    /// Possible typs of chunking
    using ChunkingType = std::variant<NoChunking, IndividualChunking>;
    /// Which mars keys form the resuling axis.
    std::vector<std::string> keys{};
    /// Defines how the Axis will be chunked.
    ChunkingType chunking{};
};

}  // namespace chunked_data_view
