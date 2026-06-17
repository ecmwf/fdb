
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

class RequestManipulationException : public eckit::Exception {

public:

    RequestManipulationException(const std::string&);
    RequestManipulationException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
