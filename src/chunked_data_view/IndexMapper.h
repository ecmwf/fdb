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

#include <cstddef>
#include <vector>

namespace chunked_data_view::index_mapping {

size_t axis_index_to_buffer_index(const std::vector<size_t>& indices, const std::vector<Axis>& axes);
std::vector<size_t> to_axis_parameter_index(const size_t& index, const Axis& axis);
}  // namespace chunked_data_view::index_mapping
