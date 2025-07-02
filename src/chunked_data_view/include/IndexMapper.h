#pragma once

#include <cstddef>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "fdb5/database/Key.h"

namespace chunked_data_view::index_mapping {

size_t linearize(const std::vector<size_t>& indices, const std::vector<Axis>& axes);
size_t linearize(const std::vector<size_t>& indices, const Axis& axes);
std::vector<size_t> delinearize(const size_t& index, const Axis& axis);
std::vector<size_t> indexInAxisParameters(const Axis& axes, const fdb5::Key& key);
}  // namespace chunked_data_view::index_mapping
