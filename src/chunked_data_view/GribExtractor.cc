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

#include <eckit/message/Reader.h>
#include <exception>

#include "chunked_data_view/IndexMapper.h"
#include "chunked_data_view/Buffer.h"
#include "eckit/exception/Exceptions.h"
#include "fdb5/database/Key.h"

namespace chunked_data_view {
DataLayout GribExtractor::layout(eckit::DataHandle& handle) const {
    eckit::message::Reader reader(handle);
    eckit::message::Message msg = reader.next();
    if (!msg) {
        // TODO(kkratz) Use proper exception, use helpful message
        throw eckit::Exception("GribExtractor::layout: Couldn't read GRIB message.");
    }
    size_t countValues = msg.getSize("values");
    return {countValues, 8};
}

void GribExtractor::writeInto(eckit::DataHandle& handle, uint8_t* out, const DataLayout& layout) const {
    // TODO(kkratz): Add error handling and length checks!
    eckit::message::Reader reader(handle);
    eckit::message::Message msg{};
    auto copyInto = reinterpret_cast<double*>(out);
    // TODO(kkratz): This just copies the date in the order as it is retrieved!
    while ((msg = reader.next())) {
        size_t countValues = msg.getSize("values");
        msg.getDoubleArray("values", copyInto, countValues);
        copyInto += countValues;
    }
}


size_t computeBufferIndex(const std::vector<Axis>& axes, const fdb5::Key& key) {

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

    return chunked_data_view::index_mapping::axis_index_to_buffer_index(result, axes);
}

void GribExtractor::writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
                              const DataLayout& layout, Buffer& buffer) const {

    bool iterator_empty = true;

    if(buffer.filled()) {
        buffer.resetBits();
    }

    while (auto res = list_iterator->next()) {

        if (!res) {
            break;
        }
        iterator_empty = false;

        const auto& key     = std::get<0>(*res);
        auto& data_handle   = std::get<1>(*res);
        const size_t offset = computeBufferIndex(axes, key);

        try {
            eckit::message::Reader reader(*data_handle);
            eckit::message::Message msg{};

            auto copyInto = buffer.dataPtr() + offset * layout.countValues;

            while ((msg = reader.next())) {
                size_t countValues = msg.getSize("values");
                msg.getDoubleArray("values", copyInto, countValues);
                buffer.setBits(offset, countValues);
                copyInto += countValues;
            }
        }
        catch (std::exception e) {
            eckit::Log::debug() << e.what() << std::endl;
        }
    }

    if(iterator_empty) {
        throw eckit::Exception("Empty iterator for request. Is the request correctly specified?");
    }

    // if(!buffer.filled()) {
    //     throw eckit::Exception("Buffer not completely filled. Either request is spanning data which is not in the FDB or data of the FDB is missing.");
    // }
}

};  // namespace chunked_data_view
