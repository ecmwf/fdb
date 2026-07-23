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

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"

namespace chunked_data_view {

/// Static factory that converts a list of AxisDefinitions into the (Axis, AxisChunks) pairs
/// consumed by ViewPart.
///
/// mapRequestToAxis() is the main entry point: for each AxisDefinition it extracts the
/// matching keyword values from the MARS request, builds the Axis, and delegates to
/// mapAxisToChunks() for the corresponding AxisChunks.
class AxisMapper {
public:

    /// Builds one (Axis, AxisChunks) pair per AxisDefinition using the keyword values
    /// found in @p mars_request. Every multi-valued keyword in the request must be covered
    /// by exactly one AxisDefinition.
    /// @throws eckit::UserError if a keyword appears in more than one AxisDefinition, or if
    ///         a multi-valued keyword is not covered by any AxisDefinition.
    static std::vector<std::pair<Axis, AxisChunks>> mapRequestToAxis(
        const metkit::mars::MarsRequest& mars_request, const std::vector<AxisDefinition>& axis_definition);

    /// Returns true if @p wishedChunkSize is a valid chunk size for @p axis.
    ///
    /// C is valid iff C = trailingProduct(k) × d, where trailingProduct(k) is the product
    /// of the cardinalities of the k fastest-varying parameters, and d divides the
    /// cardinality of the (k+1)-th parameter from the fastest end.
    ///
    /// This guarantees every chunk covers all values of the inner (faster) keys in full
    /// and sub-divides exactly one outer key evenly — no chunk may straddle a boundary
    /// between values of an inner key.
    static bool chunkSizeCheck(const Axis& axis, const size_t wishedChunkSize);

    /// Creates the AxisChunks object for @p axis given the requested chunking strategy.
    /// @throws AxisMapperException if the chunk size fails chunkSizeCheck().
    static AxisChunks mapAxisToChunks(const Axis& axis, chunked_data_view::AxisDefinition::ChunkingType);
};
}  // namespace chunked_data_view
