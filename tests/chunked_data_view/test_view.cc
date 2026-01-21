/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <chunked_data_view/ChunkedDataView.h>
#include <chunked_data_view/ChunkedDataViewBuilder.h>
#include <chunked_data_view/DataLayout.h>
#include <chunked_data_view/Extractor.h>
#include <chunked_data_view/Fdb.h>
#include <eckit/io/MemoryHandle.h>
#include <eckit/testing/Test.h>
#include <fdb5/api/helpers/FDBToolRequest.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ListIterator.h"
#include "eckit/io/DataHandle.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Inspector.h"
#include "metkit/mars/MarsRequest.h"


using fdb5::FDBToolRequest;

//==============================================================================
namespace cdv = chunked_data_view;

std::unique_ptr<eckit::DataHandle> makeHandle(const std::vector<double>& values) {
    const size_t size = values.size() * sizeof(std::decay_t<decltype(values)>::value_type) + 2 * sizeof(size_t);
    const size_t bytesPerValue = 8;
    auto handle                = std::make_unique<eckit::MemoryHandle>(size);
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
    cdv::DataLayout layout(eckit::DataHandle& handle) const override {
        cdv::DataLayout layout{};
        handle.openForRead();
        EXPECT_EQUAL(handle.read(&layout.countValues, sizeof(layout.countValues)), sizeof(layout.countValues));
        EXPECT_EQUAL(handle.read(&layout.bytesPerValue, sizeof(layout.bytesPerValue)), sizeof(layout.bytesPerValue));
        handle.close();
        return layout;
    }

    void writeInto(const metkit::mars::MarsRequest& request,
                   std::unique_ptr<chunked_data_view::ListIteratorInterface> list_iterator,
                   const std::vector<chunked_data_view::Axis>& axes, const chunked_data_view::DataLayout& layout,
                   float* ptr, size_t len, size_t expected_msg_count) const override {
        cdv::DataLayout readLayout{};


        while (auto res = list_iterator->next()) {

            if (!res) {
                break;
            }

            const auto& key   = std::get<0>(*res);
            auto& data_handle = std::get<1>(*res);
            data_handle->openForRead();

            EXPECT_EQUAL(data_handle->read(&readLayout.countValues, sizeof(layout.countValues)), 8l);
            EXPECT_EQUAL(data_handle->read(&readLayout.bytesPerValue, sizeof(layout.bytesPerValue)), 4l);
            const size_t totalBytes = layout.countValues * layout.bytesPerValue;
            EXPECT_EQUAL(data_handle->read(ptr, totalBytes), totalBytes);
        }
    };
};

CASE("ChunkedDataView | View from 1 request | Can compute shape") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = FDBToolRequest::requestsFromString(keys).at(0).request();

    const auto view =
        cdv::ChunkedDataViewBuilder(
            std::make_unique<MockFdb>([](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                                      [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                                          return std::make_unique<MockListIterator>(
                                              std::vector<std::tuple<fdb5::Key, std::vector<double>>>{std::make_tuple(
                                                  fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
                                      }))
            .addPart(keys,
                     {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                     std::make_unique<FakeExtractor>())
            .build();
    //
    // Expect to get: 4 dates, 4 times, 2 fields, 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 2, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 4, 1, 1}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{1, 1, 2, 10}));
}

CASE("ChunkedDataView | View from 2 requests | Can compute shape") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = FDBToolRequest::requestsFromString(keys).at(0).request();

    const auto view =
        cdv::ChunkedDataViewBuilder(
            std::make_unique<MockFdb>(
                [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                    return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                        std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                    });
                }))
            .addPart(keys,
                     {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{}}},
                     std::make_unique<FakeExtractor>())
            .addPart(keys,
                     {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{}}},
                     std::make_unique<FakeExtractor>())
            .extendOnAxis(2)
            .build();
    // Expect to get: 4 dates, 4 times, 2*2 fields (2 per request), 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 4, 10}));
}

CASE("ChunkedDataView | View from 2 requests | Can compute shape, combined axis") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = FDBToolRequest::requestsFromString(keys).at(0).request();

    const auto view =
        cdv::ChunkedDataViewBuilder(
            std::make_unique<MockFdb>(
                [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                    return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                        std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                    });
                }))
            .addPart(keys,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{}}},
                     std::make_unique<FakeExtractor>())
            .addPart(keys,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{}}},
                     std::make_unique<FakeExtractor>())
            .extendOnAxis(1)
            .build();
    // Expect to get: 16 date/times (4 dates * 4 times), 2*2 fields (2 per request), 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{16, 4, 10}));
}

CASE("ChunkedDataView - Can build") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = FDBToolRequest::requestsFromString(keys).at(0).request();

    EXPECT_NO_THROW(
        cdv::ChunkedDataViewBuilder(
            std::make_unique<MockFdb>(
                [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                    return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                        std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                    });
                }))
            .addPart(keys,
                     {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::IndividualChunking{}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{}}},
                     std::make_unique<FakeExtractor>())
            .build());
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
