
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

class GribExtractorException : public eckit::Exception {

public:

    GribExtractorException(const std::string&);
    GribExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
