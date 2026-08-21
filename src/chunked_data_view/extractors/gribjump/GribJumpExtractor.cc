// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/gribjump/GribJumpExtractor.h"

#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/RequestManipulation.h"
#include "chunked_data_view/Types.h"
#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/exception/GribJumpExtractorException.h"
#include "chunked_data_view/include/chunked_data_view/Fdb.h"
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
#include <sstream>
#include <vector>

namespace chunked_data_view {

GribJumpExtractor::GribJumpExtractor(std::unique_ptr<FdbInterface> fdb, std::unique_ptr<gribjump::GribJump> gj,
                                     const metkit::mars::MarsRequest& marsRequest,
                                     AxisDefinition::ChunkingType fieldChunking) :
    fdb_(std::move(fdb)), gj_(std::move(gj)), fieldChunking_(std::move(fieldChunking)) {

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

    fullCountValues_ = countValues;
    resolvedRange_ = {0, countValues};

    const size_t windowSize = resolvedRange_.second - resolvedRange_.first;

    // Resolve the per-chunk size from the fieldChunking variant.
    struct ChunkSizeVisitor {
        size_t windowSize;
        size_t operator()(const AxisDefinition::WholeAxisChunking&) const { return windowSize; }
        size_t operator()(const AxisDefinition::SingleValueChunking&) const { return 1; }
        size_t operator()(const AxisDefinition::FixedSizeChunking& c) const { return c.chunkSize; }
    };
    fieldChunkSize_ = std::visit(ChunkSizeVisitor{windowSize}, fieldChunking_);

    if (windowSize % fieldChunkSize_ != 0) {
        std::ostringstream ss;
        ss << "GribJumpExtractor: field chunk size " << fieldChunkSize_ << " does not evenly divide the window size "
           << windowSize << ".";
        throw eckit::UserError(ss.str());
    }
    fieldChunkCount_ = windowSize / fieldChunkSize_;

    // countValues      = total window size (implicit dimension extent in the Zarr array).
    // countChunkValues = per-chunk size (what extractInto() writes per field per call).
    layout_ = {windowSize, 4, fieldChunkSize_};
}

size_t GribJumpExtractor::writeInto(const std::vector<fdb5::Key>& gj_keys, gribjump::ExtractionIterator& gj_it,
                                    const WriteContext& ctx, float* ptr, size_t len) const {
    size_t messagesWritten = 0;

    for (size_t i = 0; gj_it.hasNext(); ++i) {
        auto result = gj_it.next();  // unique_ptr<ExtractionResult>
        if (!result) {
            break;
        }

        const fdb5::Key& key = gj_keys[i];
        const size_t msgIndex =
            index_mapping::computeBufferIndex(ctx.axes, key, ctx.partAxisOffset, ctx.bufferOffset, ctx.bufferExtent);

        float* dst = ptr + msgIndex * ctx.layout.countChunkValues;
        const float* end = dst + ctx.layout.countChunkValues;
        ASSERT(end - ptr <= static_cast<std::ptrdiff_t>(len));

        // GribJump returns one range per ExtractionRequest range entry (we pass one).
        assert(result->values().size() == 1);
        assert(result->mask().size() == 1);

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
    auto listIt = fdb_->inspect(request);

    std::vector<fdb5::Key> gj_keys;
    std::vector<gribjump::PathExtractionRequest> gj_requests;

    while (const std::optional<ListElement>& res = listIt->next()) {

        if (!res.has_value()) {
            break;
        }

        gj_keys.push_back(res->key);

        const auto& location_uri = res->location->fullUri();

        // Derive the per-chunk extraction range from the implicit dimension of chunkBoundingBox.
        // chunkBoundingBox.lower().back() = chunkIndex.back() * fieldChunkSize_
        // chunkBoundingBox.upper().back() = lower.back() + fieldChunkSize_ - 1  (inclusive)
        const size_t implicitStart = chunkBoundingBox.lower().back();
        const size_t implicitEnd = chunkBoundingBox.upper().back() + 1;  // half-open for GribJump
        const gribjump::Range chunkRange{resolvedRange_.first + implicitStart, resolvedRange_.first + implicitEnd};

        if (location_uri.fragment().empty()) {
            std::ostringstream ss;
            ss << "GribJumpExtractor: Empty fragment for location uri in request " << request
               << ". Can't forward the file offset.";
            throw GribJumpExtractorException(ss.str());
        }

        // File offsets can exceed 2 GB, therefore use stoll
        gribjump::PathExtractionRequest tmp(location_uri.path(), location_uri.scheme(),
                                            std::stoll(location_uri.fragment()), location_uri.host(),
                                            location_uri.port() > 0 ? location_uri.port() : 0, {chunkRange});
        gj_requests.emplace_back(tmp);
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
        buf << "GribJumpExtractor::extractInto: " << exception.what();
        throw GribJumpExtractorException(buf.str());
    }
    catch (GribJumpExtractorException& exception) {
        std::ostringstream ss;
        ss << "GribJumpExtractor::extractInto: ";
        ss << exception.what();
        ss << "Request was: " << request << std::endl;
        throw GribJumpExtractorException(ss.str());
    }
}

}  // namespace chunked_data_view
