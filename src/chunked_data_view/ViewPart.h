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

class BoundingBox {

public:

    BoundingBox();
    BoundingBox(const std::vector<size_t>& lower, const std::vector<size_t>& upper);

    size_t dimensions() const { return lower_.size(); }
    std::vector<size_t> lower() const { return lower_; }
    std::vector<size_t> upper() const { return upper_; }

    size_t entries() const;

    bool contains(const BoundingBox& other) const;

    BoundingBox subtract(const std::vector<size_t>& subtrahend) const;

    BoundingBox dropLastDimension() const;

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

    std::vector<size_t> extension() const {
        std::vector<size_t> result;

        for (size_t i = 0; i < lower_.size(); ++i) {
            result.push_back(upper_[i] - lower_[i]);
        }
        return result;
    }


    std::vector<size_t> lower_{};
    std::vector<size_t> upper_{};
};


class ViewPart {
public:

    ViewPart(const metkit::mars::MarsRequest& request, const DataLayout& data_layout,
             const std::vector<std::pair<Axis, AxisChunks>>& axes, const std::vector<size_t>& offset);

    ~ViewPart() = default;

    ViewPart(ViewPart&&) = default;
    ViewPart& operator=(ViewPart&&) = default;
    ViewPart(const ViewPart&) = delete;
    ViewPart& operator=(const ViewPart&) = delete;

    metkit::mars::MarsRequest at(const std::vector<size_t>& chunkIndex) const;
    metkit::mars::MarsRequest at(const BoundingBox& boundingBox) const;

    std::vector<AxisChunks> chunks() const { return chunks_; }
    bool isExtensible(const size_t axisIndex) const { return chunks_[axisIndex].isExtensible(); }
    const DataLayout& layout() const { return layout_; }
    const std::vector<Axis>& axes() const { return axes_; }

    std::vector<size_t> extension() const { return extension_; }  // TODO(TKR) redundant with bb
    std::vector<size_t> offset() const { return offset_; }
    const BoundingBox& boundingBox() const { return bb_; }

    size_t offsetOnAxis(size_t axisIndex) const { return offset_[axisIndex]; }
    bool isAxisChunked(size_t index) const { return true; };
    bool extensibleWith(const ViewPart& other, size_t extension_axis) const;

private:

    metkit::mars::MarsRequest requestAt(const std::vector<size_t>& chunkIndex) const;

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
