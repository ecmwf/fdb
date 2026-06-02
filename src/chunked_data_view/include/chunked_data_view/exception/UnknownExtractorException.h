#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

class UnknownExtractorException : public eckit::Exception {

public:

    UnknownExtractorException(const std::string&);
    UnknownExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
