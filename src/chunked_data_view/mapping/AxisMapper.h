
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"

namespace chunked_data_view {

class AxisMapper {
public:

    static std::vector<Axis> mapRequestToAxis(const metkit::mars::MarsRequest& mars_request,
                                              const std::vector<AxisDefinition>& axis_definition);
};
};  // namespace chunked_data_view
