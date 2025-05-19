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
#include <string>
using fdb5::FDBToolRequest;

//==============================================================================
namespace cdv = chunked_data_view;

struct MockFdb final : public cdv::Fdb {
    using RetFunc = std::function<std::unique_ptr<eckit::DataHandle>(const metkit::mars::MarsRequest&)>;
    explicit MockFdb(RetFunc func) : fn(std::move(func)) {}

    std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) override {
        return fn(request);
    };

    RetFunc fn{};
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
    void writeInto(eckit::DataHandle& handle, uint8_t* out, const cdv::DataLayout& layout) const override {
        handle.openForRead();
        cdv::DataLayout readLayout{};
        EXPECT_EQUAL(handle.read(&readLayout.countValues, sizeof(layout.countValues)), 8l);
        EXPECT_EQUAL(handle.read(&readLayout.bytesPerValue, sizeof(layout.bytesPerValue)), 8l);
        const size_t totalBytes = layout.countValues * layout.bytesPerValue;
        EXPECT_EQUAL(handle.read(out, totalBytes), totalBytes);
    }
};

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

    const auto view = cdv::ChunkedDataViewBuilder(std::make_unique<MockFdb>([](auto& _) {
                          return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
                      }))
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                    cdv::AxisDefinition{{"param"}, false}},
                                   std::make_unique<FakeExtractor>())
                          .build();
    // Expect to get: 4 dates, 4 times, 2 fields, 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 2, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 4, 1, 1}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{1, 1, 2, 10}));
}

CASE("ChunkedDataView | View from 1 request | read data") {
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

    const auto view = cdv::ChunkedDataViewBuilder(
                          std::make_unique<MockFdb>([](auto& _) { return makeHandle({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}); }))
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                    cdv::AxisDefinition{{"param"}, true}},
                                   std::make_unique<FakeExtractor>())
                          .build();
    // Expect to get: 4 dates, 4 times, 2 fields, 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 2, 10}));

    const auto& values = view->at({0, 0, 0});
    for (int val = 0; val < 10; ++val) {
        EXPECT_EQUAL(static_cast<double>(val), values[val]);
    }
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

    const auto view = cdv::ChunkedDataViewBuilder(std::make_unique<MockFdb>([](auto& _) {
                          return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
                      }))
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                    cdv::AxisDefinition{{"param"}, true}},
                                   std::make_unique<FakeExtractor>())
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                    cdv::AxisDefinition{{"param"}, true}},
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
            std::make_unique<MockFdb>([](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); }))
            .addPart(keys, {cdv::AxisDefinition{{"date", "time"}, true}, cdv::AxisDefinition{{"param"}, true}},
                     std::make_unique<FakeExtractor>())
            .addPart(keys, {cdv::AxisDefinition{{"date", "time"}, true}, cdv::AxisDefinition{{"param"}, true}},
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

    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder(
                        std::make_unique<MockFdb>([](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); }))
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                  cdv::AxisDefinition{{"param"}, true}},
                                 std::make_unique<FakeExtractor>())
                        .build());
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
