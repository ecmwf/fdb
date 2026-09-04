// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/extractors/grib/GribExtractor.h"

#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/RequestManipulation.h"
#include "chunked_data_view/Types.h"
#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/exception/GribExtractorException.h"
#include "chunked_data_view/mapping/IndexMapper.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/message/Reader.h"
#include "fdb5/database/Key.h"
#include "metkit/mars/MarsRequest.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>

namespace chunked_data_view {

GribExtractor::GribExtractor(std::unique_ptr<FdbInterface> fdb, const metkit::mars::MarsRequest& marsRequest) :
    fdb_(std::move(fdb)) {

    // Use a minimal sample request: all requested params but only the first value of every
    // other key. This lets us verify every param the user asked for without retrieving the
    // full data volume.
    const metkit::mars::MarsRequest sampleRequest =
        marsRequest.has("param") ? RequestManipulation::allParamRequest(marsRequest) : marsRequest;

    const auto& handle = fdb_->retrieve(sampleRequest);
    eckit::message::Reader reader(*handle);
    eckit::message::Message msg = reader.next();

    if (!msg) {
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }

    const size_t countValues = msg.getSize("values");

    // Sanity check: every field returned by FDB must carry a paramId that the user
    // actually requested. FDB can silently substitute a different parameter when it
    // performs on-the-fly wind derivation (e.g. returning vo/d when u/v is requested).
    // That case is not supported: the axis mapping relies on the returned keys matching
    // the request exactly. Checking all messages (not just the first) catches cases where
    // the mismatch only appears for certain parameters.
    //
    // Note: marsRequest has already been passed through FDBToolRequest::requestsFromString()
    // by ChunkedDataViewBuilder::build(), which runs metkit's TypeParam expansion pass.
    // Short param names (e.g. "v", "vo") are resolved to numeric paramId strings (e.g. "132",
    // "138") before this point, so the comparison against std::to_string(msg.getLong("paramId"))
    // is always numeric-vs-numeric.
    if (marsRequest.has("param")) {
        const auto& requestedParams = marsRequest.values("param");
        do {
            const std::string returnedParam = std::to_string(msg.getLong("paramId"));
            if (std::find(requestedParams.begin(), requestedParams.end(), returnedParam) == requestedParams.end()) {
                std::ostringstream buf;
                buf << "GribExtractor::layout: FDB returned paramId=" << returnedParam
                    << " which is not among the requested params [";
                for (size_t i = 0; i < requestedParams.size(); ++i) {
                    if (i > 0) {
                        buf << ", ";
                    }
                    buf << requestedParams[i];
                }
                buf << "]. On-the-fly field derivation (e.g. u/v from vo/d) is not supported.";
                throw GribExtractorException(buf.str());
            }
        } while ((msg = reader.next()));
    }

    layout_ = {countValues, 4, countValues};
}


// Copies field values from FDB into the output buffer, mapping each key to its
// buffer slot using the three offset/extent vectors in WriteContext.
//
// One-axis illustration (the same logic applies independently per axis):
//
//   Part axis  (6 values, e.g. param):
//   index:  0    1    2    3    4    5
//          [p0] [p1] [p2] [p3] [p4] [p5]
//                    [=========]          <- intersection (sub-range)
//          [=========]
//          partAxisOffset = 2
//          (start of intersection in the part's local axis)
//
//   Chunk buffer  (bufferExtent = 4 slots):
//   slot:   0    1    2    3
//          [  ] [  ] [  ] [  ]
//               [=========]              <- same intersection, in buffer space
//          [====]
//          bufferOffset = 1
//          (start of intersection within the chunk buffer)
//
//   For key p2  (axis.index(key) = 2):
//     local  = axis.index(key) - partAxisOffset  =  2 - 2  =  0
//     bufPos = local + bufferOffset              =  0 + 1  =  1
//
size_t GribExtractor::writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const WriteContext& ctx,
                                float* ptr, size_t len) const {

    bool iterator_empty = true;
    size_t messagesWritten = 0;

    while (auto res = list_iterator->next()) {

        if (!res) {
            break;
        }
        iterator_empty = false;

        const auto& key = res->key;
        auto data_handle = res->dataHandle();
        const size_t msgIndex =
            index_mapping::computeBufferIndex(ctx.axes, key, ctx.partAxisOffset, ctx.bufferOffset, ctx.bufferExtent);

        eckit::message::Reader reader(*data_handle);
        eckit::message::Message msg{};

        auto copyInto = ptr + msgIndex * ctx.layout.countChunkValues;
        const auto end = copyInto + ctx.layout.countChunkValues;
        ASSERT(end - ptr <= len);

        while ((msg = reader.next())) {
            if (const auto size = msg.getSize("values"); size != ctx.layout.countChunkValues) {
                std::ostringstream ss;
                ss << "GribExtractor: Unexpected field size found in GRIB message for key: " << key
                   << " expected: " << ctx.layout.countChunkValues << " found: " << size
                   << ". All fields in your view need to be of equal size.";
                throw eckit::Exception(ss.str());
            }
            msg.getFloatArray("values", copyInto, ctx.layout.countChunkValues);
            if (msg.getLong("bitmapPresent") != 0) {
                const auto gribMissing = static_cast<float>(msg.getDouble("missingValue"));
                std::replace(copyInto, copyInto + ctx.layout.countChunkValues, gribMissing, fillValue_);
            }
            messagesWritten++;
        }
    }

    if (iterator_empty) {
        throw chunked_data_view::GribExtractorException(
            "GribExtractor: Empty iterator for request. Is the request correctly specified?");
    }

    return messagesWritten;
}

size_t GribExtractor::extractInto(const ViewPart& part, const ChunkBoundingBox& chunkBoundingBox,
                                  const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                                  size_t len) const {

    const auto& chunkPartBoundingBox = chunkBoundingBox.dropLastDimension();

    ASSERT(chunkPartBoundingBox.contains(intersectionBoundingBox));
    ASSERT(part.boundingBox().contains(intersectionBoundingBox));

    const PartBoundingBox& partRelativeBoundingBox = intersectionBoundingBox.subtract(part.boundingBox().lower());

    const auto& request = part.at(partRelativeBoundingBox);

    // fdb_ is shared mutable state; see mutex_ in the header.
    const std::lock_guard<std::mutex> lock(mutex_);

    auto listIterator = fdb_->inspect(request);

    const BufferBoundingBox& bufferRelativBoundingBox = intersectionBoundingBox.subtract(chunkPartBoundingBox.lower());

    const WriteContext ctx{part.axes(), layout_, partRelativeBoundingBox.lower(), bufferRelativBoundingBox.lower(),
                           chunkPartBoundingBox.extent()};

    try {
        size_t written = writeInto(std::move(listIterator), ctx, ptr, len);
        return written;
    }
    catch (GribExtractorException& exception) {
        std::ostringstream ss;
        ss << "GribExtractor::extractInto: ";
        ss << exception.what();
        ss << "Request was: " << part.at(partRelativeBoundingBox) << std::endl;
        throw GribExtractorException(ss.str());
    }
}

};  // namespace chunked_data_view
