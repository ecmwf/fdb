#pragma once

#include "chunked_data_view/Axis.h"
namespace chunked_data_view {

class RequestManipulation {
public:

    static void updateRequest(metkit::mars::MarsRequest& request, const Axis& axis, std::size_t chunkIndex);
};
}  // namespace chunked_data_view
