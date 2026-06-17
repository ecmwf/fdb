#include "chunked_data_view/exception/BoundingBoxException.h"

namespace chunked_data_view {
BoundingBoxException::BoundingBoxException(const std::string& w) : Exception(w) {}

BoundingBoxException::BoundingBoxException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}

}  // namespace chunked_data_view
