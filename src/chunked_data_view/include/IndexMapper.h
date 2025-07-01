#pragma once

#include <cstddef>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "fdb5/database/Key.h"

namespace chunked_data_view {

class IndexMapper {
public:

    static size_t linearize(const std::vector<std::size_t>& indices, const std::vector<Axis>& axes);
    static size_t linearize(const std::vector<std::size_t>& indices, const Axis& axes);

    static std::vector<size_t> delinearize(const std::size_t& index, const Axis& axis);

    static std::vector<std::size_t> indexInAxisParameters(const Axis& axes, const fdb5::Key& key);
};
}  // namespace chunked_data_view
