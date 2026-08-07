/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include <numeric>
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

    // C is valid iff C = trailingProduct × d, where:
    //   trailingProduct = product of cardinalities of the k fastest-varying parameters, and
    //   d divides the cardinality of the (k+1)-th parameter from the fastest end.
    // This guarantees every chunk covers all values of the faster-varying keys in full and
    // evenly sub-divides one slower key — no partial inner-key groups are allowed.
    const auto& params = axis.parameters();
    const size_t n = params.size();
    size_t trailingProduct = 1;
    for (size_t k = 0; k < n; ++k) {
        const size_t card = params[n - 1 - k].values().size();
        if (wishedChunkSize % trailingProduct == 0) {
            const size_t d = wishedChunkSize / trailingProduct;
            if (card % d == 0) {
                return true;
            }
        }
        trailingProduct *= card;
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

        if (!AxisMapper::chunkSizeCheck(axis, chunks_mars_axis_extension)) {
            std::stringstream buf;
            buf << "AxisMapper::mapAxisToChunks: The chunk size must equal "
                   "(trailing product of k fastest-varying key cardinalities) × d, "
                   "where d divides the (k+1)-th key's cardinality. "
                   "The requested chunk size was: "
                << chunks_mars_axis_extension << ". The total axis size is: " << axis.size();
            throw chunked_data_view::AxisMapperException(buf.str());
        };

        const size_t chunk_extensions_single_axis = chunks_mars_axis_extension;

        // Integer ceil
        const size_t amount =
            axis.size() / chunk_extensions_single_axis + ((axis.size() % chunk_extensions_single_axis) != 0);

        return chunked_data_view::AxisChunks(chunk_extensions_single_axis, amount, false);
    }

    throw chunked_data_view::AxisMapperException("Axis::contructor: Unknown type for chunking.");
}
}  // namespace chunked_data_view
