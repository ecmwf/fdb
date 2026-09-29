// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "ViewPart.h"

#include <algorithm>
#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "metkit/mars/MarsRequest.h"

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/RequestManipulation.h"
#include "chunked_data_view/exception/BoundingBoxException.h"


namespace chunked_data_view {

BoundingBox::BoundingBox() : lower_({}), upper_({}) {};
BoundingBox::BoundingBox(const std::vector<size_t>& lower, const std::vector<size_t>& upper) {
    if (lower.size() != upper.size()) {
        std::stringstream buf;
        buf << "BoundingBox::BoundingBox: Mismatch in dimensions of lower and upper corner. Lower: " << lower.size()
            << ", Upper:" << upper.size();
        throw chunked_data_view::BoundingBoxException(buf.str());
    }

    for (size_t i = 0; i < lower.size(); ++i) {
        if (lower[i] > upper[i]) {
            std::stringstream buf;
            buf << "BoundingBox::BoundingBox: Lower: " << lower.size() << ", Upper:" << upper.size()
                << ". lower[i] must be <= upper[i].";
            throw chunked_data_view::BoundingBoxException(buf.str());
        }
    }

    lower_ = lower;
    upper_ = upper;
};

size_t BoundingBox::entries() const {
    const auto ext = extent();

    size_t prod = 1;

    for (size_t i = 0; i < ext.size(); ++i) {
        prod *= ext[i];
    }
    return prod;
}

bool BoundingBox::contains(const BoundingBox& other) const {
    ASSERT(other.dimensions() == this->dimensions());

    for (size_t i = 0; i < other.dimensions(); ++i) {
        if (other.upper()[i] > upper()[i] || other.lower()[i] < lower()[i]) {
            return false;
        }
    }

    return true;
}

BoundingBox BoundingBox::subtract(const std::vector<size_t>& subtrahend) const {
    ASSERT(subtrahend.size() == lower_.size());

    std::vector<size_t> newLower;
    std::vector<size_t> newUpper;

    for (size_t i = 0; i < lower_.size(); ++i) {
        if (lower_[i] < subtrahend[i] || upper_[i] < subtrahend[i]) {
            throw eckit::UserError("BoundingBox::subtract:: Underflow in bounding box calculation.");
        }
        newLower.push_back(lower_[i] - subtrahend[i]);
        newUpper.push_back(upper_[i] - subtrahend[i]);
    }
    return BoundingBox(newLower, newUpper);
}

BoundingBox BoundingBox::dropLastDimension() const {

    std::vector<size_t> lowerRestricted;
    std::vector<size_t> upperRestricted;

    for (size_t i = 0; i < lower_.size() - 1; ++i) {
        lowerRestricted.push_back(lower_[i]);
        upperRestricted.push_back(upper_[i]);
    }
    return BoundingBox(lowerRestricted, upperRestricted);
}

std::optional<BoundingBox> BoundingBox::intersect(const BoundingBox& other) const {
    // For two intervals [l1, u1], [l2, u2] no intersections exists if
    // u1 < l2 or l1 > u2 (separating axis)
    // If there is an intersection it's [max(l1, l2), min(u1, u2)]
    // For every axis
    std::vector<size_t> lower;
    std::vector<size_t> upper;

    for (size_t i = 0; i < lower_.size(); ++i) {
        const auto l1 = lower_[i];
        const auto u1 = upper_[i];
        const auto l2 = other.lower_[i];
        const auto u2 = other.upper_[i];

        if ((u1 < l2) || (l1 > u2)) {
            eckit::Log::debug() << "Returning empty bounding box" << std::endl;
            return std::nullopt;  // empty (separating axis theorem)
        }

        const auto lowerAxis = std::max(l1, l2);
        const auto upperAxis = std::min(u1, u2);

        lower.push_back(lowerAxis);
        upper.push_back(upperAxis);
    }

    return std::make_optional<>(BoundingBox(lower, upper));
}

ViewPart::ViewPart(const metkit::mars::MarsRequest& request, const DataLayout& data_layout,
                   const std::vector<std::pair<Axis, AxisChunks>>& axes, const std::vector<size_t>& offset) :
    request_(request), layout_(data_layout), offset_(offset) {

    extension_.reserve(axes_.size());
    chunks_.reserve(axes_.size());
    axes_.reserve(axes_.size());

    for (const auto& [axis, axis_chunks] : axes) {
        axes_.push_back(axis);
        chunks_.push_back(axis_chunks);
        extension_.push_back(axis.size());
    }

    const auto lower = offset_;
    auto upper = offset_;

    for (size_t i = 0; i < offset_.size(); ++i) {
        upper[i] += (extension_[i] - 1);
    }

    bb_ = BoundingBox(lower, upper);
}

metkit::mars::MarsRequest ViewPart::at(const PartBoundingBox& boundingBox) const {

    const auto translatedBB = bb_.subtract(bb_.lower());
    const auto contained = translatedBB.contains(boundingBox);
    if (!contained) {
        std::ostringstream msg;
        msg << "ViewPart::at: Relative part bounding box " << translatedBB << " doesn't fully contain " << boundingBox
            << std::endl;
        throw chunked_data_view::BoundingBoxException(msg.str(), Here());
    }

    return RequestManipulation::selectRequest(request_, axes_, boundingBox);
}

bool ViewPart::extensibleWith(const ViewPart& other, const size_t extension_axis) const {

    if (other.extension().size() != this->extension().size()) {
        return false;
    }

    for (size_t index = 0; index < other.extension().size(); ++index) {
        if (index == extension_axis) {
            continue;
        }
        // Checking the size of the combined axis
        // @info This is not checking whether the parameters are matching. We intentionally
        // can stitch mismatching parameters.
        if (other.extension()[index] != extension()[index]) {
            return false;
        }
    }

    return true;
}

}  // namespace chunked_data_view
