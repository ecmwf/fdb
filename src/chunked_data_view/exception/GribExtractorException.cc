
#include "chunked_data_view/exception/GribExtractorException.h"

namespace chunked_data_view {
GribExtractorException::GribExtractorException(const std::string& w) : Exception(w) {}

GribExtractorException::GribExtractorException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}

}  // namespace chunked_data_view
