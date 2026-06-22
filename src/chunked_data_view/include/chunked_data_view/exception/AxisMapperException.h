
#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

class AxisMapperException : public eckit::Exception {

public:

    AxisMapperException(const std::string&);
    AxisMapperException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
