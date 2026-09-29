// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/Axis.h"

#include <cstddef>
#include <vector>

/// Free functions for mapping MARS field keys and flat axis indices to buffer positions.
namespace chunked_data_view::index_mapping {

/// Computes the flat buffer slot index for a field identified by @p key.
///
/// For each axis the function looks up the key's position within the axis, subtracts
/// @p partAxisOffset (intersection start in part-local space), then adds @p bufferOffset
/// (intersection start in buffer space), and folds all axes into a single row-major flat
/// index using @p bufferExtent as the stride denominator.
///
/// @param axes            Ordered axes of the ViewPart.
/// @param key             MARS key of the field returned by the FDB iterator.
/// @param partAxisOffset  Per-axis start of the intersection in part-local coordinates.
/// @param bufferOffset    Per-axis start of the intersection in chunk-buffer coordinates.
/// @param bufferExtent    Per-axis size of the chunk buffer (used as strides).
/// @return Flat index into the output buffer (in units of one field, i.e. countValues floats).
size_t computeBufferIndex(const std::vector<Axis>& axes, const fdb5::Key& key,
                          const std::vector<size_t>& partAxisOffset, const std::vector<size_t>& bufferOffset,
                          const std::vector<size_t>& bufferExtent);

/// Converts a flat row-major axis index back into per-parameter sub-indices.
/// This is the inverse of the product enumeration used by Axis::index().
/// The returned vector has one entry per parameter in @p axis (last parameter varies fastest).
std::vector<size_t> to_axis_parameter_index(const size_t& index, const Axis& axis);

}  // namespace chunked_data_view::index_mapping
