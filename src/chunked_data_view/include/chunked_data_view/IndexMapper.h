#pragma once

#include <cstddef>
#include <vector>
#include "chunked_data_view/Axis.h"

namespace chunked_data_view::index_mapping {

size_t axis_index_to_buffer_index(const std::vector<size_t>& indices, const std::vector<Axis>& axes);
std::vector<size_t> to_axis_parameter_index(const size_t& index, const Axis& axis);
}  // namespace chunked_data_view::index_mapping
