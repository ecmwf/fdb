// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <chunked_data_view/ChunkedDataView.h>
#include <chunked_data_view/ChunkedDataViewBuilder.h>
#include <eckit/testing/Test.h>
#include <fdb5/api/helpers/FDBToolRequest.h>

#include <memory>
#include <string>
#include <vector>
#include "chunked_data_view/AxisDefinition.h"
#include "test_mock_helpers.h"


CASE("ChunkedDataView | FixedSizeChunking | Can compute shape") {
    // 4 dates chunked in pairs, 4 single-value times, 2 params whole-axis
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB());

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::FixedSizeChunking{2}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .build();

    // 4 dates (chunkSize=2 -> 2 chunks), 4 single times, 2 params whole, 10 values
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 2, 10}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{2, 1, 2, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{2, 4, 1, 1}));
}

CASE("ChunkedDataView | FixedSizeChunking | Invalid chunk size throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB());

    // chunkSize=0 -- explicitly forbidden
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::FixedSizeChunking{0}},
                                cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                               mock_extractor)
                      .build());

    // chunkSize=1 -- same as SingleValueChunking, redundant, but ok
    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder()
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::FixedSizeChunking{1}},
                                  cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                 mock_extractor)
                        .build());

    // chunkSize==axis size (4) -- same as WholeAxisChunking, redundant, but ok
    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder()
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::FixedSizeChunking{4}},
                                  cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                 mock_extractor)
                        .build());
}

CASE("ChunkedDataView | FixedSizeChunking | at() accesses each chunk") {
    // Same view as the shape test: 4 dates (chunkSize=2), 4 times, 2 params whole-axis
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(5));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::FixedSizeChunking{2}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::WholeAxisChunking{}}},
                                   mock_extractor)
                          .build();

    std::vector<float> buf(view->countChunkValues());

    // Chunk (0,*,0,0): dates [0,1] -- first date-pair
    EXPECT_NO_THROW(view->at({0, 0, 0, 0}, buf.data(), buf.size()));

    // Chunk (1,*,0,0): dates [2,3] -- second date-pair
    EXPECT_NO_THROW(view->at({1, 0, 0, 0}, buf.data(), buf.size()));

    // Out of bounds: only 2 date-chunks (0 and 1)
    EXPECT_THROWS(view->at({2, 0, 0, 0}, buf.data(), buf.size()));
}


CASE("ChunkedDataView | FixedSizeChunking | Combined axis with differently-sized chunk dimensions") {
    // Combined {"date","time"} axis: 4 dates x 4 times = 16 values, FixedSizeChunking{4} -> 4 chunks of 4.
    // Each chunk covers exactly one "time sweep" (1 date x all 4 times) in MARS coordinates.
    // Separate {"param"} axis: 4 params, FixedSizeChunking{2} -> 2 chunks of 2.
    // This gives chunkShape = {4, 2, 10}: the two chunked dims have *different* extents (4 vs 2),
    // demonstrating that FixedSizeChunking on a combined axis is independent of the other axes.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=10/20/30/40,time=0/6/12/18"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(9));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{4}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::FixedSizeChunking{2}}},
                                   mock_extractor)
                          .build();

    // Combined datextime axis: 16 values, chunkSize=4 -> 4 chunks.
    // Param axis: 4 values, chunkSize=2 -> 2 chunks.
    // Implicit values dimension: 10 values, always 1 chunk.
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{16, 4, 10}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{4, 2, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 2, 1}));

    std::vector<float> buf(view->countChunkValues());

    // First chunk: combined-axis [0,3] x param [0,1]
    EXPECT_NO_THROW(view->at({0, 0, 0}, buf.data(), buf.size()));
    // Interior chunk: combined-axis [4,7] x param [2,3]
    EXPECT_NO_THROW(view->at({1, 1, 0}, buf.data(), buf.size()));
    // Last chunk: combined-axis [12,15] x param [2,3]
    EXPECT_NO_THROW(view->at({3, 1, 0}, buf.data(), buf.size()));

    // Out-of-bounds on the combined axis (only 4 chunks: 0-3)
    EXPECT_THROWS(view->at({4, 0, 0}, buf.data(), buf.size()));
    // Out-of-bounds on the param axis (only 2 chunks: 0-1)
    EXPECT_THROWS(view->at({0, 2, 0}, buf.data(), buf.size()));
}

CASE("ChunkedDataView | FixedSizeChunking | Combined axis with differently-sized chunk dimensions 2") {
    // Combined {"date","time"} axis: 4x3=12 values, FixedSizeChunking{3} -> 4 chunks of 3.
    // {"levelist"} axis: 10 values, FixedSizeChunking{5} -> 2 chunks of 5.
    // {"param"} axis: 6 values, FixedSizeChunking{3} -> 2 chunks of 3.
    // All three chunked dims have different extents (3, 5, 3), exercising the general case.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=ml,levelist=100/200/300/400/500/600/700/800/900/1000,"
        "param=10/20/30/40/50/60,time=0/6/12"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(46));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                                    cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::FixedSizeChunking{5}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::FixedSizeChunking{3}}},
                                   mock_extractor)
                          .build();

    // Combined datextime: 12, chunkSize=3 -> 4 chunks
    // levelist: 10, chunkSize=5 -> 2 chunks
    // param: 6, chunkSize=3 -> 2 chunks
    // Implicit values axis: 10, always 1 chunk
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{12, 10, 6, 10}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{3, 5, 3, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 2, 2, 1}));

    std::vector<float> buf(view->countChunkValues());

    // First chunk: combined [0,2] x levelist [0,4] x param [0,2]
    EXPECT_NO_THROW(view->at({0, 0, 0, 0}, buf.data(), buf.size()));
    // Interior chunk: combined [3,5] x levelist [5,9] x param [3,5]
    EXPECT_NO_THROW(view->at({1, 1, 1, 0}, buf.data(), buf.size()));
    // Last valid chunk: combined [9,11] x levelist [5,9] x param [3,5]
    EXPECT_NO_THROW(view->at({3, 1, 1, 0}, buf.data(), buf.size()));

    // Out-of-bounds on combined axis (4 chunks: 0-3)
    EXPECT_THROWS(view->at({4, 0, 0, 0}, buf.data(), buf.size()));
    // Out-of-bounds on levelist axis (2 chunks: 0-1)
    EXPECT_THROWS(view->at({0, 2, 0, 0}, buf.data(), buf.size()));
    // Out-of-bounds on param axis (2 chunks: 0-1)
    EXPECT_THROWS(view->at({0, 0, 2, 0}, buf.data(), buf.size()));
    // Out-of-bounds on implicit values axis (1 chunk: 0)
    EXPECT_THROWS(view->at({0, 0, 0, 1}, buf.data(), buf.size()));

    // Invalid chunk sizes: non-divisors of the axis extent must throw at build time
    // levelist has 10 values -> FixedSizeChunking{3} does not divide evenly
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                                cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::FixedSizeChunking{3}}},
                               mock_extractor)
                      .build());

    // param has 6 values -> FixedSizeChunking{4} does not divide evenly
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                                cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::FixedSizeChunking{5}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::FixedSizeChunking{4}}},
                               mock_extractor)
                      .build());
}

CASE("ChunkedDataView | FixedSizeChunking | Multi part with combined axis with differently-sized chunk dimensions 2") {
    // Part 1 (sfc, param=110/120/130):
    //   {"date","time"} combined: 12 values, FixedSizeChunking{3} -> 4 chunks of 3.
    //   {"param"}: 3 values, FixedSizeChunking{3} -> 1 chunk of 3.
    // Part 2 (ml, levelistxparam=10x6=60):
    //   {"date","time"} combined: 12 values, FixedSizeChunking{3} -> 4 chunks of 3.
    //   {"levelist","param"} combined: 60 values, FixedSizeChunking{3} -> 20 chunks of 3.
    // Extended on axis 1: 3 (sfc param) + 60 (ml levelistxparam) = 63, 1+20=21 chunks.
    const std::string keys_sfc{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,param=110/120/130,time=0/6/12"};

    const std::string keys_ml{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=ml,levelist=100/200/300/400/500/600/700/800/900/1000,"
        "param=10/20/30/40/50/60,time=0/6/12"};

    auto mock_extractor = std::make_shared<FakeExtractor>(createMockFDB(10));

    const auto view =
        cdv::ChunkedDataViewBuilder()
            .addPart(keys_sfc,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::FixedSizeChunking{3}}},
                     mock_extractor)
            .addPart(keys_ml,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::FixedSizeChunking{3}},
                      {cdv::AxisDefinition{{"levelist", "param"}, cdv::AxisDefinition::FixedSizeChunking{3}}}},
                     mock_extractor)
            .extendOnAxis(1)
            .build();

    // Part 1 (sfc): datextime=12/IC{3}->4 chunks, param=3/IC{3}->1 chunk
    // Part 2 (ml):  datextime=12/IC{3}->4 chunks, levelist x param=60/IC{3}->20 chunks
    // extendOnAxis(1): axis-1 total = 3+60=63, chunks = 1+20=21
    // Implicit values axis: 10, 1 chunk
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{12, 63, 10}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{3, 3, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{4, 21, 1}));

    std::vector<float> buf(view->countChunkValues());

    // First chunk: part-1 slice, combined [0,2] x axis-1 [0,2]
    EXPECT_NO_THROW(view->at({0, 0, 0}, buf.data(), buf.size()));
    // First chunk of part-2 extent: combined [0,2] x axis-1 [3,5]
    EXPECT_NO_THROW(view->at({0, 1, 0}, buf.data(), buf.size()));
    // Last valid chunk: combined [9,11] x axis-1 [60,62]
    EXPECT_NO_THROW(view->at({3, 20, 0}, buf.data(), buf.size()));

    // Out-of-bounds on combined axis (4 chunks: 0-3)
    EXPECT_THROWS(view->at({4, 0, 0}, buf.data(), buf.size()));
    // Out-of-bounds on axis-1 (21 chunks: 0-20)
    EXPECT_THROWS(view->at({0, 21, 0}, buf.data(), buf.size()));
    // Out-of-bounds on implicit values axis (1 chunk: 0)
    EXPECT_THROWS(view->at({0, 0, 1}, buf.data(), buf.size()));
}


int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
