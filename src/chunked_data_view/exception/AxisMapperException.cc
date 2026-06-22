
#include "chunked_data_view/exception/AxisMapperException.h"

namespace chunked_data_view {
AxisMapperException::AxisMapperException(const std::string& w) : Exception(w) {}

AxisMapperException::AxisMapperException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}

}  // namespace chunked_data_view
