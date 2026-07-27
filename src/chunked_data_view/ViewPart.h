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

#include "Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <ostream>
#include <vector>

namespace chunked_data_view {

/// An axis-aligned, closed bounding box in the ChunkedDataView index space.
///
/// Each dimension is represented as the interval [lower, upper]. Coordinates are
/// unsigned indices into the combined N-dimensional view (not into a single part).
///
/// Used to describe both the footprint of a ViewPart within the full view and the
/// extent of a requested chunk, so that intersections can be computed efficiently.
class BoundingBox {

public:

    BoundingBox();
    /// @param lower Inclusive lower corner in each dimension.
    /// @param upper Exclusive upper corner in each dimension (must have same size as lower).
    BoundingBox(const std::vector<size_t>& lower, const std::vector<size_t>& upper);

    /// Number of dimensions.
    size_t dimensions() const { return lower_.size(); }
    /// Inclusive lower corner.
    std::vector<size_t> lower() const { return lower_; }
    /// Inclusive upper corner.
    std::vector<size_t> upper() const { return upper_; }
    std::vector<size_t> extent() const {
        std::vector<size_t> extent;
        for (size_t i = 0; i < dimensions(); ++i) {
            extent.push_back(upper_[i] - lower_[i] + 1);
        }
        return extent;
    }

    /// Total number of integer lattice points inside the box (product of per-axis extents).
    size_t entries() const;

    /// Returns true if every corner of @p other lies within this box.
    bool contains(const BoundingBox& other) const;

    /// Returns a new bounding box shifted by subtracting @p subtrahend from both corners.
    BoundingBox subtract(const std::vector<size_t>& subtrahend) const;

    /// Returns a copy of this bounding box with the last dimension removed.
    BoundingBox dropLastDimension() const;

    /// Returns the intersection with @p other, or std::nullopt if they do not overlap.
    std::optional<BoundingBox> intersect(const BoundingBox& other) const;

    bool operator==(const BoundingBox& b) const { return lower_ == b.lower() && upper_ == b.upper(); }
    bool operator!=(const BoundingBox& right) const { return !operator==(right); }

    friend std::ostream& operator<<(std::ostream& cout, BoundingBox& c) {
        cout << "[";

        for (size_t i = 0; i < c.dimensions() - 1; ++i) {
            cout << c.lower()[i] << ", ";
        }

        cout << c.lower()[c.dimensions() - 1] << "] x [";

        for (size_t i = 0; i < c.dimensions() - 1; ++i) {
            cout << c.upper()[i] << ", ";
        }

        cout << c.upper()[c.dimensions() - 1] << "]";

        return cout;
    }

private:

    /// Per-axis extents (upper - lower + 1for each dimension).
    std::vector<size_t> extension() const {
        std::vector<size_t> result;

        for (size_t i = 0; i < lower_.size(); ++i) {
            result.push_back(upper_[i] - lower_[i] + 1);
        }
        return result;
    }


    std::vector<size_t> lower_{};
    std::vector<size_t> upper_{};
};


/// Represents one logical slice of the ChunkedDataView backed by a single MARS request.
///
/// A ViewPart maps a MARS request onto a rectangular region of the N-dimensional view.
/// Its axes describe how the multi-valued MARS keywords (e.g. date, time, param) form
/// the dimensions of that region, and its chunking configuration controls how that
/// region is subdivided when data is accessed.
///
/// The last dimension is always the implicit field-values dimension derived from
/// DataLayout::countValues; it is never represented by a MARS keyword and is always
/// returned as a single, unchunked block.
///
/// Multiple ViewParts are combined by ChunkedDataViewImpl to form the full view.
/// Parts that share all non-extension axes can be stitched together along one
/// extension axis (see extensibleWith()).
///
/// Coordinates used throughout this class (offsets, bounding boxes) are expressed
/// in the global ChunkedDataView index space, not in part-local coordinates.
class ViewPart {
public:

    /// Constructs a ViewPart.
    /// @param request     The MARS request that describes the data covered by this part.
    /// @param data_layout Number of values and bytes-per-value for each field.
    /// @param axes        Ordered list of (Axis, AxisChunks) pairs, one per non-values dimension.
    ///                    Each keyword with more than one value must be covered by exactly one axis.
    /// @param offset      Position of the lower corner of this part in the global view index space,
    ///                    one entry per axis (excluding the implicit values dimension).
    ViewPart(const metkit::mars::MarsRequest& request, const DataLayout& data_layout,
             const std::vector<std::pair<Axis, AxisChunks>>& axes, const std::vector<size_t>& offset);

    ~ViewPart() = default;

    ViewPart(ViewPart&&) = default;
    ViewPart& operator=(ViewPart&&) = default;
    ViewPart(const ViewPart&) = delete;
    ViewPart& operator=(const ViewPart&) = delete;

    /// Returns the MARS sub-request that covers the fields within @p boundingBox.
    /// The bounding box must intersect the part's own bounding box.
    metkit::mars::MarsRequest at(const BoundingBox& boundingBox) const;

    /// Chunking descriptors for each axis (excluding the implicit values dimension).
    std::vector<AxisChunks> chunks() const { return chunks_; }

    /// Returns true if the axis at @p axisIndex is marked as extensible,
    /// i.e. additional parts may be stitched onto this part along that axis.
    bool isExtensible(const size_t axisIndex) const { return chunks_[axisIndex].isExtensible(); }

    /// Field layout (countValues and bytesPerValue) shared by all fields in this part.
    const DataLayout& layout() const { return layout_; }

    /// Ordered axes that define the non-values dimensions of this part.
    const std::vector<Axis>& axes() const { return axes_; }

    /// Number of entries (fields or values) along each dimension, including the implicit values dimension as the last
    /// entry.
    std::vector<size_t> extension() const { return extension_; }  // TODO(TKR) redundant with bb

    /// Position of the lower corner of this part in the global view index space.
    std::vector<size_t> offset() const { return offset_; }

    /// Bounding box of this part in the global view index space (half-open: [offset, offset + extension)).
    const BoundingBox& boundingBox() const { return bb_; }

    /// Offset of this part along a single axis in the global view index space.
    size_t offsetOnAxis(size_t axisIndex) const { return offset_[axisIndex]; }

    bool isAxisChunked(size_t index) const { return true; };

    /// Returns true if this part and @p other can be stitched together along @p extension_axis,
    /// i.e. their extents match on every axis except the extension axis.
    bool extensibleWith(const ViewPart& other, size_t extension_axis) const;

private:

    // Each keyword defines a potential axis in the resulting view.
    // No axis needs to be created if the cardinality is one.
    // Each keyword with cardinality greater than 1 needs to be covered by exactly one
    // axis definition
    metkit::mars::MarsRequest request_{};
    std::vector<Axis> axes_{};
    std::vector<AxisChunks> chunks_;
    DataLayout layout_{};

    std::vector<size_t> extension_{};  // extension in each dimension, counting entries
    std::vector<size_t> offset_{};     // offset in chunked data view
    BoundingBox bb_;                   // bounding box in chunked data view coord
};

}  // namespace chunked_data_view
