
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"

namespace chunked_data_view {

/// Static factory that converts a list of AxisDefinitions into the (Axis, AxisChunks) pairs
/// consumed by ViewPart.
///
/// mapRequestToAxis() is the main entry point: for each AxisDefinition it extracts the
/// matching keyword values from the MARS request, builds the Axis, and delegates to
/// mapAxisToChunks() for the corresponding AxisChunks.
class AxisMapper {
public:

    /// Builds one (Axis, AxisChunks) pair per AxisDefinition using the keyword values
    /// found in @p mars_request. Every multi-valued keyword in the request must be covered
    /// by exactly one AxisDefinition.
    /// @throws eckit::UserError if a keyword appears in more than one AxisDefinition, or if
    ///         a multi-valued keyword is not covered by any AxisDefinition.
    static std::vector<std::pair<Axis, AxisChunks>> mapRequestToAxis(
        const metkit::mars::MarsRequest& mars_request, const std::vector<AxisDefinition>& axis_definition);

    /// Returns true if @p wishedChunkSize is a valid chunk size for @p axis: it must equal
    /// the product of value counts of one or more trailing (fastest-varying) parameters, or
    /// evenly divide the fastest-varying parameter's value count.
    static bool chunkSizeCheck(const Axis& axis, const size_t wishedChunkSize);

    /// Creates the AxisChunks object for @p axis given the requested chunking strategy.
    /// @throws AxisMapperException if the chunk size fails chunkSizeCheck().
    static AxisChunks mapAxisToChunks(const Axis& axis, chunked_data_view::AxisDefinition::ChunkingType);
};
};  // namespace chunked_data_view
