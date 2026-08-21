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
#include <numeric>
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

/// Minimal concrete fdb5::FieldLocation serving an in-memory field.
struct MockFieldLocation final : public fdb5::FieldLocation {

    explicit MockFieldLocation(std::vector<double> values) : values_(std::move(values)) {}

    eckit::DataHandle* dataHandle() const override { return makeHandle(values_).release(); }

    std::shared_ptr<const fdb5::FieldLocation> make_shared() const override {
        return std::make_shared<const MockFieldLocation>(values_);
    }

    void visit(fdb5::FieldLocationVisitor&) const override {}

    std::vector<double> values_;
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

        return chunked_data_view::ListElement{std::get<0>(*iter_),
                                              std::make_shared<const MockFieldLocation>(std::get<1>(*iter_))};
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

/// @param fieldAmount  how many entries the inspect() iterator holds. NOTE: MockListIterator
///                     pre-increments before returning, so it yields fieldAmount - 1 elements.
/// @param countValues  values per field, i.e. the extent of the implicit (grid-point) axis.
///                     Vary it to build parts whose grids disagree.
inline std::shared_ptr<MockFdb> createMockFDB(size_t fieldAmount = 1, size_t countValues = 10) {
    std::vector<double> values(countValues);
    std::iota(values.begin(), values.end(), 1.0);
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

    explicit FakeExtractorDefinition(std::shared_ptr<chunked_data_view::FdbInterface> mock_fdb) :
        mock_fdb_(std::move(mock_fdb)) {}

    /// No-op: the mock FDB ignores configuration entirely.
    void setDefaultIfUnset(const std::optional<std::filesystem::path>& fdbConfigPath) override {}

    std::unique_ptr<ExtractorDefinition> copy() const override {
        return std::make_unique<FakeExtractorDefinition>(*this);
    }

    std::unique_ptr<cdv::Extractor> buildExtractor(const metkit::mars::MarsRequest&) const override {
        return std::make_unique<FakeExtractor>(mock_fdb_);
    }
};
