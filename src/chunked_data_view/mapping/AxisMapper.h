
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"

namespace chunked_data_view {

class AxisMapper {
public:

    static std::vector<std::pair<Axis, AxisChunks>> mapRequestToAxis(
        const metkit::mars::MarsRequest& mars_request, const std::vector<AxisDefinition>& axis_definition);

    static bool chunkSizeCheck(const Axis& axis, const size_t wishedChunkSize);
    static AxisChunks mapAxisToChunks(const Axis& axis, chunked_data_view::AxisDefinition::ChunkingType);
};
};  // namespace chunked_data_view
