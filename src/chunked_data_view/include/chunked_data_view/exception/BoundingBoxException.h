
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

class BoundingBoxException : public eckit::Exception {

public:

    BoundingBoxException(const std::string&);
    BoundingBoxException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
