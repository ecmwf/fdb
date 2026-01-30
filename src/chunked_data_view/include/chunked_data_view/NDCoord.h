/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once
#include <cassert>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>
#include <string_view>
#include <iostream>

namespace chunked_data_view {
/// N-dimensional coordinate with tag-based type safety.
///
/// Base template for indices, shapes, and offsets. The Tag parameter creates
/// distinct types that are otherwise structurally identical, preventing
/// accidental misuse (e.g., passing a Shape where an Index is expected).
///
/// @tparam Tag Empty struct used to distinguish coordinate types
///
/// @note All coordinates are stored as int32_t. For arrays exceeding 2^31-1
///       elements per dimension, consider switching to int64_t.
///
/// @see GlobalIndex, PartIndex, LocalIndex, ChunkIndex, Shape, Offset
template <typename Tag>
struct NDCoord {
    std::vector<int32_t> data;

    NDCoord() = default;

    /// Construct a coordinate with given dimensionality, zero-initialized.
    /// @param ndim Number of dimensions
    explicit NDCoord(size_t ndim) : data(ndim, 0) {}

    /// Construct from initializer list.
    /// @param init Brace-enclosed list of coordinate values
    NDCoord(std::initializer_list<int32_t> init) : data(init) {}

    /// Construct from vector.
    /// @param vector representing index information
    NDCoord(std::vector<int32_t> vec) : data(std::move(vec)) {}

    /// @return Number of dimensions
    size_t ndim() const { return data.size(); }

    /// @param i Dimension index
    /// @return Mutable reference to coordinate at dimension i
    int32_t& operator[](size_t i) { return data[i]; }

    /// @param i Dimension index
    /// @return Const reference to coordinate at dimension i
    const int32_t& operator[](size_t i) const { return data[i]; }

    auto begin() { return data.begin(); }
    auto end() { return data.end(); }
    auto begin() const { return data.begin(); }
    auto end() const { return data.end(); }

    bool operator==(const NDCoord& other) const { return data == other.data; };
};

// --- Tags ---
template <typename Tag>
struct tag_name {
    static constexpr std::string_view value = "Unknown";
};

#define DEFINE_COORD_TAG(Tag, Name)                \
    struct Tag {};                                 \
    template <>                                    \
    struct tag_name<Tag> {                         \
        static constexpr std::string_view value = Name; \
    }

DEFINE_COORD_TAG(GlobalTag, "Global");  ///< Tag for global array indices
DEFINE_COORD_TAG(PartTag, "Part");      ///< Tag for indices within a part
DEFINE_COORD_TAG(ChunkTag, "Chunk");    ///< Tag for chunk grid indices
DEFINE_COORD_TAG(LocalTag, "Local");    ///< Tag for indices within a chunk
DEFINE_COORD_TAG(ShapeTag, "Shape");    ///< Tag for dimensional extents
DEFINE_COORD_TAG(OffsetTag, "Offset");  ///< Tag for displacements
DEFINE_COORD_TAG(RegionTag, "Region");

template <typename T>
std::ostream& operator<<(std::ostream& out, const NDCoord<T>& c) {
    out << "NDCoord<" << tag_name<T>::value << ">[";
    for (const auto& val : c) {
        if (&val != &c[0]) {
            out << ", ";
        }
        out << val;
    }
    out << "]";
    return out;
}

// --- Type aliases ---

using GlobalIndex = NDCoord<GlobalTag>;  ///< Position in the full array
using PartIndex   = NDCoord<PartTag>;    ///< Position within a part's local space
using ChunkIndex  = NDCoord<ChunkTag>;   ///< Position in the chunk grid
using LocalIndex  = NDCoord<LocalTag>;   ///< Position within a single chunk
using RegionIndex = NDCoord<RegionTag>;
using Shape       = NDCoord<ShapeTag>;   ///< Dimensional extents
using Offset      = NDCoord<OffsetTag>;  ///< Displacement vector

/// Axis-aligned N-dimensional region in a specific coordinate space.
template <typename IndexT>
struct Region {
    IndexT start;  ///< Inclusive lower bound
    IndexT end;    ///< Exclusive upper bound

    Region() = default;
    Region(IndexT s, IndexT e) : start(std::move(s)), end(std::move(e)) {}

    size_t ndim() const { return start.ndim(); }

    Shape shape() const {
        Shape s(ndim());
        for (size_t d = 0; d < ndim(); ++d) {
            s[d] = end[d] - start[d];
        }
        return s;
    }

    bool empty() const {
        for (size_t d = 0; d < ndim(); ++d) {
            if (end[d] <= start[d])
                return true;
        }
        return false;
    }
};

using GlobalRegion = Region<GlobalIndex>;  ///< A Region in the global scope.
using PartRegion   = Region<PartIndex>;    ///< A Region in a parts scope.
using LocalRegion  = Region<RegionIndex>;


// --- Factory functions ---

/// Create a global region from a chunk position.
///
/// @param chunk       Chunk position in the grid
/// @param chunk_shape Dimensions of each chunk
/// @return Region in global coordinates [start, end)
inline GlobalRegion make_region(const ChunkIndex& chunk, const Shape& chunk_shape) {
    GlobalIndex start(chunk.ndim());
    GlobalIndex end(chunk.ndim());

    for (size_t d = 0; d < chunk.ndim(); ++d) {
        start[d] = chunk[d] * chunk_shape[d];
        end[d]   = start[d] + chunk_shape[d];
    }
    return {start, end};
}

/// Create a part-local region from a global query and part extent.
///
/// Intersects the query with the part's extent, then converts to part-local coordinates.
///
/// @param query       Region to fetch (e.g., chunk region in global coords)
/// @param part_extent Part's coverage in global coordinates
/// @return Region in part-local coordinates, empty if no overlap
inline PartRegion make_region(const GlobalRegion& query, const GlobalRegion& part_extent) {
    size_t ndim = query.ndim();

    // Intersect
    GlobalIndex istart(ndim);
    GlobalIndex iend(ndim);
    for (size_t d = 0; d < ndim; ++d) {
        istart[d] = std::max(query.start[d], part_extent.start[d]);
        iend[d]   = std::min(query.end[d], part_extent.end[d]);
    }

    // Convert to part-local (subtract part origin)
    PartIndex pstart(ndim);
    PartIndex pend(ndim);
    for (size_t d = 0; d < ndim; ++d) {
        pstart[d] = istart[d] - part_extent.start[d];
        pend[d]   = iend[d] - part_extent.start[d];
    }

    return {pstart, pend};
}

// --- Helper functions ---
namespace {
inline int64_t extent(const Shape& s, size_t d) {
    return s[d];
}

template <typename IndexT>
int64_t extent(const Region<IndexT>& r, size_t d) {
    return r.end[d] - r.start[d];
}
}  // namespace

// --- Utility functions ---

/// Compute total number of elements in a shape or region.
///
/// @param shape Dimensional extents
/// @return Product of all dimensions
template <typename T>
inline int64_t volume(const T& region_like) {
    int64_t result = 1;
    for (size_t axis = 0; axis < region_like.ndim(); ++axis) {
        const int64_t ext = extent(region_like, axis);
        result *= ext;
    }
    return result;
}

inline std::optional<GlobalRegion> intersection(const GlobalRegion& a, const GlobalRegion& b) {
    assert(a.ndim() == b.ndim());

    GlobalIndex start(a.ndim());
    GlobalIndex end(a.ndim());

    for (size_t d = 0; d < a.ndim(); ++d) {
        start[d] = std::max(a.start[d], b.start[d]);
        end[d]   = std::min(a.end[d], b.end[d]);

        if (start[d] >= end[d]) {
            return std::nullopt;  // empty intersection
        }
    }
    return GlobalRegion{start, end};
}

inline LocalRegion to_local(const GlobalRegion& region, const GlobalRegion& reference) {
    assert(region.ndim() == reference.ndim());

    RegionIndex start(region.ndim());
    RegionIndex end(region.ndim());

    for (size_t d = 0; d < region.ndim(); ++d) {
        start[d] = region.start[d] - reference.start[d];
        end[d]   = region.end[d] - reference.start[d];
    }
    return {start, end};
}

// Get the part of 'region' that lies within 'reference', in local coords
inline std::optional<LocalRegion> to_local_clipped(const GlobalRegion& region, const GlobalRegion& reference) {
    auto isect = intersection(region, reference);
    if (!isect)
        return std::nullopt;
    return to_local(*isect, reference);
}


}  // namespace chunked_data_view
