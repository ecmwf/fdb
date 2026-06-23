/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <chunked_data_view/DataLayout.h>
#include <chunked_data_view/Extractor.h>
#include <chunked_data_view/Fdb.h>
#include <eckit/io/MemoryHandle.h>
#include <eckit/testing/Test.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <vector>
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/ViewPart.h"
#include "eckit/io/DataHandle.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Inspector.h"
#include "metkit/mars/MarsRequest.h"

namespace cdv = chunked_data_view;

inline std::unique_ptr<eckit::DataHandle> makeHandle(const std::vector<double>& values) {
    const size_t size = values.size() * sizeof(std::decay_t<decltype(values)>::value_type) + 2 * sizeof(size_t);
    const size_t bytesPerValue = 8;
    auto handle = std::make_unique<eckit::MemoryHandle>(size);
    size_t _{};
    handle->openForWrite(_);

    size_t countValues = values.size();
    handle->write(&countValues, sizeof(size_t));
    handle->write(&bytesPerValue, sizeof(size_t));
    handle->write(values.data(), values.size() * sizeof(std::decay_t<decltype(values)>::value_type));
    handle->close();
    return handle;
};

struct MockListIterator final : public chunked_data_view::ListIteratorInterface {

    using vec2 = std::vector<std::tuple<fdb5::Key, std::vector<double>>>;
    vec2 data_;
    vec2::const_iterator iter_;

    MockListIterator(vec2 data) : data_(std::move(data)), iter_(std::begin(data_)) {};

    std::optional<std::tuple<fdb5::Key, std::unique_ptr<eckit::DataHandle>>> next() {
        iter_++;

        if (std::end(data_) == iter_) {
            return std::nullopt;
        }

        return std::make_tuple(std::get<0>(*iter_), makeHandle(std::get<1>(*iter_)));
    };
};


struct MockFdb final : public cdv::FdbInterface {
    using RetFunc = std::function<std::unique_ptr<eckit::DataHandle>(const metkit::mars::MarsRequest&)>;
    using InsFunc =
        std::function<std::unique_ptr<chunked_data_view::ListIteratorInterface>(const metkit::mars::MarsRequest&)>;

    explicit MockFdb(RetFunc retFunc, InsFunc insFunc) : retFn(std::move(retFunc)), insFn(std::move(insFunc)) {}

    std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) override {
        return retFn(request);
    };

    std::unique_ptr<chunked_data_view::ListIteratorInterface> inspect(
        const metkit::mars::MarsRequest& request) override {
        return insFn(request);
    }

    RetFunc retFn{};
    InsFunc insFn{};
};

struct FakeExtractor : public cdv::Extractor {

    std::shared_ptr<chunked_data_view::FdbInterface> mock_;

    explicit FakeExtractor(std::shared_ptr<chunked_data_view::FdbInterface> mock_fdb) : mock_(mock_fdb) {}

    cdv::DataLayout layout(const metkit::mars::MarsRequest& mars_request) const override {
        const auto handle = mock_->retrieve(mars_request);
        cdv::DataLayout layout{};
        handle->openForRead();
        EXPECT_EQUAL(handle->read(&layout.countValues, sizeof(layout.countValues)), sizeof(layout.countValues));
        EXPECT_EQUAL(handle->read(&layout.bytesPerValue, sizeof(layout.bytesPerValue)), sizeof(layout.bytesPerValue));
        handle->close();
        return layout;
    }

    size_t extractInto(const chunked_data_view::ViewPart& part,
                       const chunked_data_view::ChunkedDataViewPartBoundingBox& chunkBoundingBox,
                       const chunked_data_view::ChunkedDataViewPartBoundingBox& intersectionBoundingBox, float* ptr,
                       size_t len) const override {

        const chunked_data_view::PartBoundingBox& partRelativeBoundingBox =
            intersectionBoundingBox.subtract(part.boundingBox().lower());

        const auto& request = part.at(partRelativeBoundingBox);
        auto listIterator = mock_->inspect(request);

        size_t written = 0;

        while (listIterator->next().has_value()) {
            written++;
        }

        return written;
    };
};
