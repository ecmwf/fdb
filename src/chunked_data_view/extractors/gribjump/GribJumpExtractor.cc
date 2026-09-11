// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/gribjump/GribJumpExtractor.h"

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/RequestManipulation.h"
#include "chunked_data_view/Types.h"
#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/exception/GribJumpExtractorException.h"
#include "chunked_data_view/mapping/IndexMapper.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/message/Reader.h"
#include "fdb5/database/Key.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/api/ExtractionIterator.h"

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>

namespace chunked_data_view {

GribJumpExtractor::GribJumpExtractor(std::unique_ptr<FdbInterface> fdb, std::unique_ptr<gribjump::GribJump> gj,
                                     const metkit::mars::MarsRequest& marsRequest,
                                     AxisDefinition::ChunkingType fieldChunking) :
    fdb_(std::move(fdb)), gj_(std::move(gj)) {

    // Use a minimal sample request (mirrors GribExtractor constructor).
    const metkit::mars::MarsRequest sampleRequest =
        marsRequest.has("param") ? RequestManipulation::allParamRequest(marsRequest) : marsRequest;

    const auto& handle = fdb_->retrieve(sampleRequest);

    eckit::message::Reader reader(*handle);
    eckit::message::Message msg = reader.next();

    if (!msg) {
        throw eckit::Exception("GribJumpExtractor::layout: Couldn't read GRIB message.");
    }

    const size_t countValues = msg.getSize("values");
    gridHash_ = msg.getString("md5GridSection");

    // Same paramId sanity check as GribExtractor constructor.
    if (marsRequest.has("param")) {
        const auto& requestedParams = marsRequest.values("param");
        do {
            const std::string returnedParam = std::to_string(msg.getLong("paramId"));
            if (std::find(requestedParams.begin(), requestedParams.end(), returnedParam) == requestedParams.end()) {
                std::ostringstream buf;
                buf << "GribJumpExtractor::layout: FDB returned paramId=" << returnedParam
                    << " which is not among the requested params [";
                for (size_t i = 0; i < requestedParams.size(); ++i) {
                    if (i > 0) {
                        buf << ", ";
                    }
                    buf << requestedParams[i];
                }
                buf << "]. On-the-fly field derivation (e.g. u/v from vo/d) is not supported.";
                throw GribJumpExtractorException(buf.str());
            }
        } while ((msg = reader.next()));
    }

    // The whole field is the window: there is no sub-range selection.
    const size_t windowSize = countValues;

    // Resolve the per-chunk size from the fieldChunking variant.
    struct ChunkSizeVisitor {
        size_t windowSize;
        size_t operator()(const AxisDefinition::WholeAxisChunking&) const { return windowSize; }
        size_t operator()(const AxisDefinition::SingleValueChunking&) const { return 1; }
        size_t operator()(const AxisDefinition::FixedSizeChunking& c) const { return c.chunkSize; }
    };
    const size_t fieldChunkSize = std::visit(ChunkSizeVisitor{windowSize}, fieldChunking);

    if (fieldChunkSize == 0) {
        throw eckit::UserError("GribJumpExtractor: field chunk size must be greater than zero.");
    }

    if (windowSize % fieldChunkSize != 0) {
        std::ostringstream ss;
        ss << "GribJumpExtractor: field chunk size " << fieldChunkSize << " does not evenly divide the window size "
           << windowSize << ".";
        throw eckit::UserError(ss.str());
    }

    // countValues      = total window size (implicit dimension extent in the Zarr array).
    // countChunkValues = per-chunk size (what extractInto() writes per field per call).
    layout_ = {windowSize, 4, fieldChunkSize};
}

size_t GribJumpExtractor::writeInto(const std::vector<fdb5::Key>& gj_keys, gribjump::ExtractionIterator& gj_it,
                                    const WriteContext& ctx, float* ptr, size_t len) const {
    size_t messagesWritten = 0;

    for (size_t i = 0; gj_it.hasNext(); ++i) {
        auto result = gj_it.next();  // unique_ptr<ExtractionResult>
        if (!result) {
            break;
        }

        // gribjump::LocalGribJump::collect_results() builds its vector by walking the requests
        // in order, so result i belongs to gj_requests[i] and hence gj_keys[i]. Checked, not
        // assumed: a change upstream would otherwise scatter fields into the wrong slots.
        ASSERT(i < gj_keys.size());

        const fdb5::Key& key = gj_keys.at(i);
        const size_t msgIndex =
            index_mapping::computeBufferIndex(ctx.axes, key, ctx.partAxisOffset, ctx.bufferOffset, ctx.bufferExtent);

        float* dst = ptr + msgIndex * ctx.layout.countChunkValues;
        const float* end = dst + ctx.layout.countChunkValues;
        ASSERT(end - ptr <= static_cast<std::ptrdiff_t>(len));

        ASSERT(result->values().size() == 1);
        ASSERT(result->mask().size() == 1);

        const auto& vals = result->values()[0];  // vector<double>
        const auto& mask = result->mask()[0];    // vector<bitset<64>>

        ASSERT(vals.size() == ctx.layout.countChunkValues);

        for (size_t j = 0; j < vals.size(); ++j) {
            const size_t word = j / 64;
            const size_t bit = j % 64;
            // GribJump bitmap convention: bit set (1) = valid, bit clear (0) = missing.
            dst[j] = mask[word][bit] ? static_cast<float>(vals[j]) : fillValue_;
        }

        ++messagesWritten;
    }

    return messagesWritten;
}

size_t GribJumpExtractor::extractInto(const ViewPart& part, const ChunkBoundingBox& chunkBoundingBox,
                                      const ChunkedDataViewPartBoundingBox& intersectionBB, float* ptr,
                                      size_t len) const {
    const ChunkedDataViewPartBoundingBox chunkPartBoundingBox = chunkBoundingBox.dropLastDimension();

    ASSERT(chunkPartBoundingBox.contains(intersectionBB));
    ASSERT(part.boundingBox().contains(intersectionBB));

    const PartBoundingBox& partRelBB = intersectionBB.subtract(part.boundingBox().lower());
    const BufferBoundingBox& bufRelBB = intersectionBB.subtract(chunkPartBoundingBox.lower());

    const metkit::mars::MarsRequest request = part.at(partRelBB);

    // Derive the extraction range from the implicit dimension of chunkBoundingBox. It is the
    // same for every field in this chunk, so it is built once rather than per field.
    //   chunkBoundingBox.lower().back() = chunkIndex.back() * layout_.countChunkValues
    //   chunkBoundingBox.upper().back() = lower.back() + countChunkValues - 1  (inclusive)
    // gribjump::Range is half-open, hence the +1 on the upper bound.
    const gribjump::Range chunkRange{chunkBoundingBox.lower().back(), chunkBoundingBox.upper().back() + 1};

    // fdb_ and gj_ are shared mutable state; see mutex_ in the header.
    const std::lock_guard<std::mutex> lock(mutex_);

    auto listIt = fdb_->inspect(request);

    std::vector<fdb5::Key> gj_keys;
    std::vector<gribjump::PathExtractionRequest> gj_requests;

    while (const auto res = listIt->next()) {

        gj_keys.push_back(res->key);

        const auto& location_uri = res->location->fullUri();

        if (location_uri.fragment().empty()) {
            std::ostringstream ss;
            ss << "GribJumpExtractor: Empty fragment for location uri in request " << request
               << ". Can't forward the file offset.";
            throw GribJumpExtractorException(ss.str());
        }

        // File offsets can exceed 2 GB, therefore use stoll.
        size_t fieldOffset = 0;
        try {
            fieldOffset = static_cast<size_t>(std::stoll(location_uri.fragment()));
        }
        catch (const std::exception& e) {
            std::ostringstream ss;
            ss << "GribJumpExtractor: Could not parse the file offset '" << location_uri.fragment()
               << "' from the location uri in request " << request << ": " << e.what();
            throw GribJumpExtractorException(ss.str());
        }

        gj_requests.emplace_back(location_uri.path(), location_uri.scheme(), fieldOffset, location_uri.host(),
                                 location_uri.port() > 0 ? location_uri.port() : 0,
                                 std::vector<gribjump::Range>{chunkRange}, gridHash_);
    }

    if (gj_keys.empty()) {
        std::ostringstream ss;
        ss << "GribJumpExtractor: Empty iterator for request " << request << ". Is the request correctly specified?";
        throw GribJumpExtractorException(ss.str());
    }

    try {
        gribjump::ExtractionIterator gj_it = gj_->extract(gj_requests);

        const WriteContext ctx{part.axes(), layout_, partRelBB.lower(), bufRelBB.lower(),
                               chunkPartBoundingBox.extent()};

        return writeInto(gj_keys, gj_it, ctx, ptr, len);
    }
    catch (eckit::SeriousBug& exception) {
        std::ostringstream buf;
        buf << "GribJumpExtractor::extractInto: " << exception.what() << ". Request was: " << request;
        throw GribJumpExtractorException(buf.str());
    }
}

}  // namespace chunked_data_view
