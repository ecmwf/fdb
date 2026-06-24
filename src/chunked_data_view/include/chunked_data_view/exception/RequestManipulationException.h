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

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by RequestManipulation when a bounding box cannot be mapped to a single MARS request
/// (e.g. the box is not aligned with a sub-axis boundary, which would require multiple requests).
class RequestManipulationException : public eckit::Exception {

public:

    RequestManipulationException(const std::string&);
    RequestManipulationException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
