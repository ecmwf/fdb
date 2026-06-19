/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "GribExtractor.h"

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/Fdb.h"
#include "chunked_data_view/IndexMapper.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/ViewPart.h"
#include "chunked_data_view/exception/GribExtractorException.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/message/Reader.h"
#include "fdb5/database/Key.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <vector>


namespace chunked_data_view {

GribExtractor::GribExtractor(const std::shared_ptr<FdbInterface> fdb) : fdb_(fdb) {}

DataLayout GribExtractor::layout(const metkit::mars::MarsRequest& mars_request) const {

    const auto& handle = fdb_->retrieve(mars_request);
    eckit::message::Reader reader(*handle);
    eckit::message::Message msg = reader.next();

    if (!msg) {
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }

    size_t countValues = msg.getSize("values");
    return {countValues, 4};
}


size_t GribExtractor::writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const WriteContext& ctx,
                                float* ptr, size_t len) const {

    bool iterator_empty = true;
    size_t messagesWritten = 0;

    while (auto res = list_iterator->next()) {

        if (!res) {
            break;
        }
        iterator_empty = false;

        const auto& key = std::get<0>(*res);
        auto& data_handle = std::get<1>(*res);
        const size_t msgIndex =
            index_mapping::computeBufferIndex(ctx.axes, key, ctx.partAxisOffset, ctx.bufferOffset, ctx.bufferExtent);

        eckit::message::Reader reader(*data_handle);
        eckit::message::Message msg{};

        auto copyInto = ptr + msgIndex * ctx.layout.countValues;
        const auto end = copyInto + ctx.layout.countValues;
        ASSERT(end - ptr <= len);

        while ((msg = reader.next())) {
            if (const auto size = msg.getSize("values"); size != ctx.layout.countValues) {
                std::ostringstream ss;
                ss << "GribExractor: Unexpected field size found in GRIB message for key: " << key
                   << " expected: " << ctx.layout.countValues << " found: " << size
                   << ". All fields in your view need to be of equal size.";
                throw eckit::Exception(ss.str());
            }
            msg.getFloatArray("values", copyInto, ctx.layout.countValues);
            messagesWritten++;
        }
    }

    if (iterator_empty) {
        throw chunked_data_view::GribExtractorException(
            "GribExtractor: Empty iterator for request. Is the request correctly specified?");
    }

    return messagesWritten;
}

size_t GribExtractor::extractInto(const ViewPart& part, const ChunkedDataViewPartBoundingBox& chunkBoundingBox,
                                  const ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                                  size_t len) const {
    ASSERT(chunkBoundingBox.contains(intersectionBoundingBox));
    ASSERT(part.boundingBox().contains(intersectionBoundingBox));

    const PartBoundingBox& partRelativeBoundingBox = intersectionBoundingBox.subtract(part.boundingBox().lower());

    const auto& request = part.at(partRelativeBoundingBox);
    auto listIterator = fdb_->inspect(request);

    const BufferBoundingBox& bufferRelativBoundingBox = intersectionBoundingBox.subtract(chunkBoundingBox.lower());

    const WriteContext ctx{part.axes(), part.layout(), partRelativeBoundingBox.lower(),
                           bufferRelativBoundingBox.lower(), chunkBoundingBox.extent()};

    try {
        size_t written = writeInto(std::move(listIterator), ctx, ptr, len);
        return written;
    }
    catch (GribExtractorException& exception) {
        std::ostringstream ss;
        ss << exception.what();
        ss << "Request was: " << part.at(partRelativeBoundingBox) << std::endl;
        throw GribExtractorException(ss.str());
    }
}

};  // namespace chunked_data_view
