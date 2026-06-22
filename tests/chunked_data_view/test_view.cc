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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/ViewPart.h"
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

    // If
    const auto request = FDBToolRequest::requestsFromString(keys).at(0).request();
    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(FakeExtractor(std::move(mock_fdb)));

    // Then
    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .build();

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

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });
    auto fake_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   fake_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   fake_extractor)
                          .extendOnAxis(2)
                          .build();
    // Expect to get: 4 dates, 4 times, 2*2 fields (2 per request), 10 values per field (implicit axis)
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 4, 10}));
}

CASE("ChunkedDataView | View from 2 requests | NoChunking on extension axis") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // MockListIterator skips first entry, so provide N+1 entries to return N messages
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    // 4 dates, 4 times, 2+2=4 params (NoChunking), 10 values per field
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 4, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 4, 1, 1}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{1, 1, 4, 10}));

    // at() should succeed -- each part contributes 2 params x 10 values = 20 floats
    std::vector<float> buf(view->countChunkValues());
    EXPECT_NO_THROW(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
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

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(1)
                          .build();
    // Expect to get: 16 date/times (4 dates * 4 times), 2*2 fields (2 per request), 10 values per field (implicit
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

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto fake_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder()
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                 fake_extractor)
                        .build());
}

CASE("ChunkedDataView | build | No data in FDB throws user-facing error") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_shared<MockFdb>(
        [](auto& _) -> std::unique_ptr<eckit::DataHandle> { throw eckit::Exception("FDB: no data"); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> { return nullptr; });

    auto fake_extractor = std::make_shared<FakeExtractor>(mock_fdb);


    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                               fake_extractor)
                      .build());
}

CASE("ChunkedDataView | at | Wrong index dimension throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .build();

    // View has 4 dimensions (date, time, param, values) -> chunks has 4 entries
    std::vector<float> buf(view->countChunkValues());

    // Too few dimensions
    EXPECT_THROWS(view->at({0, 0}, buf.data(), buf.size()));

    // Too many dimensions
    EXPECT_THROWS(view->at({0, 0, 0, 0, 0}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | at | Out-of-bounds chunk index throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .build();

    // chunks = {4, 4, 1, 1} (4 dates, 4 times, 1 param-chunk, 1 value-chunk)
    std::vector<float> buf(view->countChunkValues());

    // Valid index at the boundary -- should NOT throw
    EXPECT_NO_THROW(view->at({3, 3, 0, 0}, buf.data(), buf.size()));

    // Date index out of bounds (4 >= 4)
    EXPECT_THROWS(view->at({4, 0, 0, 0}, buf.data(), buf.size()));

    // Time index out of bounds (4 >= 4)
    EXPECT_THROWS(view->at({0, 4, 0, 0}, buf.data(), buf.size()));

    // Param chunk index out of bounds (1 >= 1)
    EXPECT_THROWS(view->at({0, 0, 1, 0}, buf.data(), buf.size()));

    // Value chunk index out of bounds (1 >= 1)
    EXPECT_THROWS(view->at({0, 0, 0, 1}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | at | Partial read throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // MockListIterator skips first entry: 2 entries -> returns 1 message.
            // View expects 2 (2 params, NoChunking) -> partial read.
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .build();

    std::vector<float> buf(view->countChunkValues());
    EXPECT_THROWS(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | at | Partial read in multi-part extension throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // Returns 1 message, but each part expects 2 (2 params, NoChunking)
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})});
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    std::vector<float> buf(view->countChunkValues());
    EXPECT_THROWS(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | View from 3 requests | Can compute shape and access") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    // 4 dates, 4 times, 2+2+2=6 params (NoChunking), 10 values per field
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 6, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 4, 1, 1}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{1, 1, 6, 10}));

    std::vector<float> buf(view->countChunkValues());
    EXPECT_NO_THROW(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
