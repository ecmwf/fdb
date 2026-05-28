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
#include <eckit/testing/Test.h>
#include <fdb5/api/helpers/FDBToolRequest.h>

#include <memory>
#include <string>
#include <vector>
#include "chunked_data_view/AxisDefinition.h"
#include "test_mock_helpers.h"


using fdb5::FDBToolRequest;

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
    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB());

    // Then
    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
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


    auto fake_extractor = std::make_shared<FakeExtractor>(createMockFDB());

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

CASE("ChunkedDataView | View from 2 requests | WholeAxisChunking on extension axis") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(3));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    // 4 dates, 4 times, 2+2=4 params (WholeAxisChunking), 10 values per field
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

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB());

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

CASE("ChunkedDataViewBuilder | build | Calling build twice throws") {
    // build() used to move the shared_ptr<Extractor> out of the builder's internal parts_ list, making a second call
    // UB. The builder now retains ownership so build() can be called multiple times safely.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB());

    cdv::ChunkedDataViewBuilder builder;
    builder.addPart(keys,
                    {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                     cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                     cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                    mock_extractor);

    EXPECT_NO_THROW(builder.build());
    EXPECT_NO_THROW(builder.build());
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

    auto fake_extractor = std::make_shared<FakeExtractor>(createMockFDB());

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
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                               fake_extractor)
                      .build());
}

CASE("ChunkedDataView | at | Wrong index dimension throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(3));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
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

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(3));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
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

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(2));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
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

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(2));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    std::vector<float> buf(view->countChunkValues());
    EXPECT_THROWS(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | at | Partial read error path uses part-local coordinates") {
    // Regression test for the error path in ChunkedDataViewImpl::at.
    // Part 2 sits at global param offset 2. Before the fix, the error path called
    // part.at() with global bounding box coordinates, causing an out-of-range lookup
    // inside Part 2's two-element param axis. After the fix it uses part-local coords.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    // createMockFDB(1) returns 0 messages (MockListIterator pre-increments before returning).
    // SingleValueChunking expects 1 message per chunk -> mismatch triggers the error path.
    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(1));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::SingleValueChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    // shape = {4, 4, 4, 10}: chunk {0,0,2,0} falls entirely inside Part 2 (param offset = 2).
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 4, 10}));
    std::vector<float> buf(view->countChunkValues());

    // Must throw eckit::UserError ("retrieved 0 of 1"), not an out-of-range or underflow error.
    EXPECT_THROWS(view->at({0, 0, 2, 0}, buf.data(), buf.size()));
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

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(3));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .extendOnAxis(2)
                          .build();

    // 4 dates, 4 times, 2+2+2=6 params (WholeAxisChunking), 10 values per field
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 6, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 4, 1, 1}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{1, 1, 6, 10}));

    std::vector<float> buf(view->countChunkValues());
    EXPECT_NO_THROW(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
