// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

namespace chunked_data_view {

class BoundingBox;

/// Bounding box of a Zarr chunk in the global ChunkedDataView index space.
using ChunkedDataViewPartBoundingBox = chunked_data_view::BoundingBox;

/// Bounding box of a Zarr chunk in the global ChunkedDataView index space (incl. implicit dimension).
using ChunkBoundingBox = chunked_data_view::BoundingBox;

/// Bounding box expressed in the coordinate space of a single ViewPart
/// (i.e. with the part's offset subtracted so that the part's own lower corner is the origin).
using PartBoundingBox = chunked_data_view::BoundingBox;

/// Bounding box expressed in the coordinate space of the output buffer
/// (i.e. relative to the chunk's lower corner).
using BufferBoundingBox = chunked_data_view::BoundingBox;
}  // namespace chunked_data_view
