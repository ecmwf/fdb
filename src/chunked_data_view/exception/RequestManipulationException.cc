#include "chunked_data_view/exception/RequestManipulationException.h"

namespace chunked_data_view {
RequestManipulationException::RequestManipulationException(const std::string& w) : Exception(w) {}

RequestManipulationException::RequestManipulationException(const std::string& w, const eckit::CodeLocation& l) :
    Exception(w, l) {}

}  // namespace chunked_data_view
