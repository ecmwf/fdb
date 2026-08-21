// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
#include "chunked_data_view/Types.h"
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

    std::optional<chunked_data_view::ListElement> next() override {
        iter_++;

        if (std::end(data_) == iter_) {
            return std::nullopt;
        }

        return chunked_data_view::ListElement{std::get<0>(*iter_), nullptr};
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

inline std::shared_ptr<MockFdb> createMockFDB(size_t fieldAmount = 1) {
    const std::vector<double> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    return std::make_shared<MockFdb>(
        [values](auto& _) { return makeHandle(values); },
        [fieldAmount, values](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>(
                fieldAmount, std::make_tuple(fdb5::Key(), values)));
        });
}

struct FakeExtractor : public cdv::Extractor {

    std::shared_ptr<chunked_data_view::FdbInterface> mock_;

    explicit FakeExtractor(std::shared_ptr<chunked_data_view::FdbInterface> mock_fdb) : mock_(mock_fdb) {
        // The mock's retrieve() ignores the request, so a default-constructed one suffices.
        const auto handle = mock_->retrieve(metkit::mars::MarsRequest{});
        handle->openForRead();
        EXPECT_EQUAL(handle->read(&layout_.countValues, sizeof(layout_.countValues)), sizeof(layout_.countValues));
        EXPECT_EQUAL(handle->read(&layout_.bytesPerValue, sizeof(layout_.bytesPerValue)),
                     sizeof(layout_.bytesPerValue));
        handle->close();
        layout_.countChunkValues = layout_.countValues;  // full field = single implicit chunk
    }

    size_t extractInto(const chunked_data_view::ViewPart& part,
                       const chunked_data_view::ChunkBoundingBox& chunkBoundingBox,
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

/// ExtractorDefinition backed by a shared mock FDB.
/// buildExtractor() creates a fresh FakeExtractor each call (satisfying unique_ptr
/// ownership), while all copies share the same mock FDB instance so multi-part tests
/// can share a single createMockFDB() result across several addPart() calls.
struct FakeExtractorDefinition : public cdv::ExtractorDefinition {
    std::shared_ptr<chunked_data_view::FdbInterface> mock_fdb_;

    explicit FakeExtractorDefinition(std::shared_ptr<chunked_data_view::FdbInterface> mock_fdb)
        : mock_fdb_(std::move(mock_fdb)) {}

    std::unique_ptr<cdv::Extractor> buildExtractor(const metkit::mars::MarsRequest&) const override {
        return std::make_unique<FakeExtractor>(mock_fdb_);
    }
};
