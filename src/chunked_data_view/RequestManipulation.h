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

#include "metkit/mars/MarsRequest.h"

#include <cstddef>

namespace chunked_data_view {

class RequestManipulation {
public:

    static void updateRequest(metkit::mars::MarsRequest& request, const Axis& axis, size_t chunkIndex);
};
}  // namespace chunked_data_view
