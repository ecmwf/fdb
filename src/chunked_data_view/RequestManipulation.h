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

#include "chunked_data_view/ViewPart.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>

namespace chunked_data_view {

/// Static utilities for restricting a MARS request to the sub-range described by a
/// part-local bounding box.
///
/// selectRequest() narrows every axis in one call; updateRequest() handles one axis at a
/// time by translating flat part-local indices back into the concrete keyword values needed
/// by FDB (e.g. converting index 3 on a [date × time] axis to date=20200102, time=0).
class RequestManipulation {
public:

    /// Returns a copy of @p request with each keyword restricted to the values implied by
    /// @p boundingBox. @p boundingBox must be in part-local coordinates (origin at the
    /// part's lower corner) and must have one dimension per entry in @p axes.
    static metkit::mars::MarsRequest selectRequest(const metkit::mars::MarsRequest& request,
                                                   const std::vector<Axis>& axes, const BoundingBox& boundingBox);

    /// Restricts the keyword(s) covered by @p axis in @p request to the elements at
    /// flat part-local indices [@p lowerIndex, @p upperIndex] (inclusive).
    /// @throws RequestManipulationException if the range spans a sub-axis boundary that
    ///         would require multiple MARS requests to retrieve.
    static void updateRequest(metkit::mars::MarsRequest& request, const Axis& axis, size_t lowerIndex,
                              size_t upperIndex);
};
}  // namespace chunked_data_view
