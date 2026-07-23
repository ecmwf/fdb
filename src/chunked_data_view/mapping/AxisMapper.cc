#include <set>
#include <sstream>

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/exception/AxisMapperException.h"
#include "chunked_data_view/mapping/AxisMapper.h"
#include "eckit/exception/Exceptions.h"
#include "metkit/mars/MarsRequest.h"

namespace chunked_data_view {

std::vector<std::pair<Axis, AxisChunks>> AxisMapper::mapRequestToAxis(
    const metkit::mars::MarsRequest& mars_request, const std::vector<AxisDefinition>& axis_defintion) {

    std::vector<std::pair<Axis, AxisChunks>> resulting_axes;

    std::set<std::string> processedKeywords{};
    for (const auto& axis : axis_defintion) {

        std::vector<Parameter> parameters{};
        parameters.reserve(axis.keys.size());
        for (const auto& key : axis.keys) {
            if (processedKeywords.count(key) != 0) {
                throw eckit::UserError("ViewPart::ViewPart:Keyword already mapped by another axis");
            }
            processedKeywords.insert(key);
            parameters.emplace_back(std::make_tuple(key, mars_request.values(key)));
        }
        const Axis new_axis = {parameters};
        const AxisChunks new_axis_chunks = AxisMapper::mapAxisToChunks(new_axis, axis.chunking);

        resulting_axes.emplace_back(std::make_pair<>(new_axis, new_axis_chunks));
    }

    for (const auto& p : mars_request.parameters()) {
        if (p.count() > 1 && processedKeywords.count(p.name()) != 1) {
            std::ostringstream ss;
            ss << "ViewPart::ViewPart:Keyword " << p.name() << " has " << p.count()
               << " values but is not mapped by an axis.";
            throw eckit::UserError(ss.str());
        }
    }

    return resulting_axes;
}

bool AxisMapper::chunkSizeCheck(const Axis& axis, const size_t wishedChunkSize) {

    if (wishedChunkSize == 0) {
        throw eckit::UserError("AxisMapper:chunkSizeCheck: Chunk size needs to be positive.");
    }

    const auto parameter_amount = axis.parameters().size();

    size_t prod = 1;

    for (std::size_t i = parameter_amount; i-- > 0;) {
        if (wishedChunkSize == prod) {
            return true;
        }
        prod *= axis.parameters()[i].values().size();
    }

    if (wishedChunkSize == prod) {
        return true;
    }

    // If the wished chunk size is a divisor of the fasted varying dimensions, this is also okay
    if (axis.parameters()[axis.parameters().size() - 1].values().size() % wishedChunkSize == 0) {
        return true;
    }

    return false;
}

AxisChunks AxisMapper::mapAxisToChunks(const Axis& axis,
                                       chunked_data_view::AxisDefinition::ChunkingType chunking_type) {

    if (std::holds_alternative<AxisDefinition::WholeAxisChunking>(chunking_type)) {
        return chunked_data_view::AxisChunks(axis.size(), 1, true);
    }
    else if (std::holds_alternative<AxisDefinition::SingleValueChunking>(chunking_type)) {
        return chunked_data_view::AxisChunks(1, axis.size(), false);
    }
    else if (std::holds_alternative<AxisDefinition::FixedSizeChunking>(chunking_type)) {
        auto chunks_mars_axis_extension = std::get<AxisDefinition::FixedSizeChunking>(chunking_type).chunkSize;

        size_t prod = 1;
        std::vector<size_t> potentialChunkSizes = {1};

        // Use post-decrement idiom: i starts at parameter_amount, decrements before use,
        // and exits cleanly when i reaches 0 (avoids size_t underflow wrap-around).
        for (std::size_t i = axis.parameters().size(); i-- > 0;) {
            prod *= axis.parameters()[i].values().size();
            potentialChunkSizes.push_back(prod);
        }

        if (!AxisMapper::chunkSizeCheck(axis, chunks_mars_axis_extension)) {
            std::stringstream buf;
            buf << "Axis::contructor: Chunking needs to be a divider of the product of the axis sizes, starting from "
                   "the most varying one. The suggested chunk size was: "
                << chunks_mars_axis_extension << ". Potential chunk sizes are: " << potentialChunkSizes;
            throw chunked_data_view::AxisMapperException(buf.str());
        };

        // The minimal chunking has to be the multiplicative of all chunk sizes, as the axis is a mapping of the
        // individual axis in MARS world, e.g. date [4] chunk_extension (2), time [3] chunk_extension (2) -> date_time
        // [12]
        const size_t chunk_extensions_single_axis = chunks_mars_axis_extension;

        // Integer ceil
        const size_t amount =
            axis.size() / chunk_extensions_single_axis + ((axis.size() % chunk_extensions_single_axis) != 0);

        return chunked_data_view::AxisChunks(chunk_extensions_single_axis, amount, false);
    }

    throw chunked_data_view::AxisMapperException("Axis::contructor: Unknown type for chunking.");
}
}  // namespace chunked_data_view
