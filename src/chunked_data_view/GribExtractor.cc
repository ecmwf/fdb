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
#include "chunked_data_view/IndexMapper.h"
#include "chunked_data_view/ListIterator.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/message/Reader.h"
#include "fdb5/database/Key.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <vector>


namespace chunked_data_view {
DataLayout GribExtractor::layout(eckit::DataHandle& handle) const {
    eckit::message::Reader reader(handle);
    eckit::message::Message msg = reader.next();
    if (!msg) {
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }
    size_t countValues = msg.getSize("values");
    return {countValues, 4};
}

size_t computeBufferIndex(const std::vector<Axis>& axes, const fdb5::Key& key, size_t extensionAxisIdx = SIZE_MAX,
                          size_t combinedExtSize = 0, size_t extensionOffset = 0) {
    std::vector<size_t> result;
    result.reserve(axes.size());

    for (const Axis& axis : axes) {

        if (axis.isChunked()) {
            result.push_back(0);
            continue;
        }

        result.emplace_back(axis.index(key));
    }

    ASSERT(result.size() == axes.size());

    return chunked_data_view::index_mapping::axis_index_to_buffer_index(result, axes, extensionAxisIdx,
                                                                        combinedExtSize, extensionOffset);
}

size_t GribExtractor::writeInto(const metkit::mars::MarsRequest& request,
                                std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
                                const DataLayout& layout, float* ptr, size_t len, size_t extensionAxisIdx,
                                size_t combinedExtSize, size_t extensionOffset) const {

    bool iterator_empty = true;
    size_t messagesWritten = 0;

    while (auto res = list_iterator->next()) {

        if (!res) {
            break;
        }
        iterator_empty = false;

        const auto& key = std::get<0>(*res);
        auto& data_handle = std::get<1>(*res);
        const size_t msgIndex = computeBufferIndex(axes, key, extensionAxisIdx, combinedExtSize, extensionOffset);

        eckit::message::Reader reader(*data_handle);
        eckit::message::Message msg{};

        auto copyInto = ptr + msgIndex * layout.countValues;
        const auto end = copyInto + layout.countValues;
        ASSERT(end - ptr <= len);

        while ((msg = reader.next())) {
            if (const auto size = msg.getSize("values"); size != layout.countValues) {
                std::ostringstream ss;
                ss << "GribExractor: Unexpected field size found in GRIB message for key: " << key
                   << " expected: " << layout.countValues << " found: " << size
                   << ". All fields in your view need to be of equal size.";
                throw eckit::Exception(ss.str());
            }
            msg.getFloatArray("values", copyInto, layout.countValues);
            messagesWritten++;
        }
    }

    if (iterator_empty) {
        std::ostringstream ss;
        ss << "GribExtractor: Empty iterator for request: " << request << ". Is the request correctly specified?";
        throw eckit::Exception(ss.str());
    }

    return messagesWritten;
}
};  // namespace chunked_data_view
