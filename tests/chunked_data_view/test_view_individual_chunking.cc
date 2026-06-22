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
#include "chunked_data_view/ListIterator.h"
#include "chunked_data_view/ViewPart.h"
#include "eckit/io/DataHandle.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Inspector.h"
#include "metkit/mars/MarsRequest.h"

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


CASE("ChunkedDataView | IndividualChunking | Can compute shape") {
    // 4 dates chunked in pairs, 4 single-value times, 2 params whole-axis
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

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
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{2}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                   mock_extractor)
                          .build();

    // 4 dates (chunkSize=2 -> 2 chunks), 4 single times, 2 params whole, 10 values
    EXPECT_EQUAL(view->shape(), (std::vector<size_t>{4, 4, 2, 10}));
    EXPECT_EQUAL(view->chunkShape(), (std::vector<size_t>{2, 1, 2, 10}));
    EXPECT_EQUAL(view->chunks(), (std::vector<size_t>{2, 4, 1, 1}));
}

CASE("ChunkedDataView | IndividualChunking | Invalid chunk size throws") {
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });
    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    // chunkSize=0 -- explicitly forbidden
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{0}},
                                cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                               mock_extractor)
                      .build());

    // chunkSize=1 -- same as SingleValueChunking, redundant, but ok
    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder()
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{1}},
                                  cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                 mock_extractor)
                        .build());

    // chunkSize==axis size (4) -- same as NoChunking, redundant, but ok
    EXPECT_NO_THROW(cdv::ChunkedDataViewBuilder()
                        .addPart(keys,
                                 {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{4}},
                                  cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                  cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
                                 mock_extractor)
                        .build());
}

CASE("ChunkedDataView | IndividualChunking | at() accesses each chunk") {
    // Same view as the shape test: 4 dates (chunkSize=2), 4 times, 2 params whole-axis
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=v/u,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // Skip-first iterator: 3 entries -> returns 2 messages (= 2 params, NoChunking)
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date"}, cdv::AxisDefinition::IndividualChunking{2}},
                                    cdv::AxisDefinition{{"time"}, cdv::AxisDefinition::SingleValueChunking{}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::NoChunking{}}},
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


CASE("ChunkedDataView | IndividualChunking | Combined axis with differently-sized chunk dimensions") {
    // Combined {"date","time"} axis: 4 dates x 4 times = 16 values, IndividualChunking{4} -> 4 chunks of 4.
    // Each chunk covers exactly one "time sweep" (1 date x all 4 times) in MARS coordinates.
    // Separate {"param"} axis: 4 params, IndividualChunking{2} -> 2 chunks of 2.
    // This gives chunkShape = {4, 2, 10}: the two chunked dims have *different* extents (4 vs 2),
    // demonstrating that IndividualChunking on a combined axis is independent of the other axes.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,"
        "param=10/20/30/40,time=0/6/12/18"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // Each chunk covers combined-axis[4] x param[2] = 8 messages.
            // MockListIterator skips the first entry, so provide 9 entries to deliver 8 messages.
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            });
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{4}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{2}}},
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

CASE("ChunkedDataView | IndividualChunking | Combined axis with differently-sized chunk dimensions 2") {
    // Combined {"date","time"} axis: 4x3=12 values, IndividualChunking{3} -> 4 chunks of 3.
    // {"levelist"} axis: 10 values, IndividualChunking{5} -> 2 chunks of 5.
    // {"param"} axis: 6 values, IndividualChunking{3} -> 2 chunks of 3.
    // All three chunked dims have different extents (3, 5, 3), exercising the general case.
    const std::string keys{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=ml,levelist=100/200/300/400/500/600/700/800/900/1000,"
        "param=10/20/30/40/50/60,time=0/6/12"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // Each chunk covers combined-axis[3] x levelist[5] x param[3] = 45 messages.
            // MockListIterator skips the first entry, so 46 entries delivers 45 messages.
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>(
                46, std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})));
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view = cdv::ChunkedDataViewBuilder()
                          .addPart(keys,
                                   {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{3}},
                                    cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::IndividualChunking{5}},
                                    cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{3}}},
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
    // levelist has 10 values -> IndividualChunking{3} does not divide evenly
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{3}},
                                cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::IndividualChunking{3}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{3}}},
                               mock_extractor)
                      .build());

    // param has 6 values -> IndividualChunking{4} does not divide evenly
    EXPECT_THROWS(cdv::ChunkedDataViewBuilder()
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{3}},
                                cdv::AxisDefinition{{"levelist"}, cdv::AxisDefinition::IndividualChunking{5}},
                                cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{4}}},
                               mock_extractor)
                      .build());
}

CASE("ChunkedDataView | IndividualChunking | Multi part with combined axis with differently-sized chunk dimensions 2") {
    // Part 1 (sfc, param=110/120/130):
    //   {"date","time"} combined: 12 values, IndividualChunking{3} -> 4 chunks of 3.
    //   {"param"}: 3 values, IndividualChunking{3} -> 1 chunk of 3.
    // Part 2 (ml, levelistxparam=10x6=60):
    //   {"date","time"} combined: 12 values, IndividualChunking{3} -> 4 chunks of 3.
    //   {"levelist","param"} combined: 60 values, IndividualChunking{3} -> 20 chunks of 3.
    // Extended on axis 1: 3 (sfc param) + 60 (ml levelistxparam) = 63, 1+20=21 chunks.
    const std::string keys_sfc{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=sfc,param=110/120/130,time=0/6/12"};

    const std::string keys_ml{
        "type=an,domain=g,expver=0001,stream=oper,"
        "date=2020-01-01/to/2020-01-04,levtype=ml,levelist=100/200/300/400/500/600/700/800/900/1000,"
        "param=10/20/30/40/50/60,time=0/6/12"};

    auto mock_fdb = std::make_unique<MockFdb>(
        [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
        [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
            // Each chunk covers combined-axis[3] x param[3] = 9 messages.
            // MockListIterator skips the first entry, so 10 entries delivers 9 messages.
            return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>(
                10, std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})));
        });

    auto mock_extractor = std::make_shared<FakeExtractor>(std::move(mock_fdb));

    const auto view =
        cdv::ChunkedDataViewBuilder()
            .addPart(keys_sfc,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{3}},
                      cdv::AxisDefinition{{"param"}, cdv::AxisDefinition::IndividualChunking{3}}},
                     mock_extractor)
            .addPart(keys_ml,
                     {cdv::AxisDefinition{{"date", "time"}, cdv::AxisDefinition::IndividualChunking{3}},
                      {cdv::AxisDefinition{{"levelist", "param"}, cdv::AxisDefinition::IndividualChunking{3}}}},
                     mock_extractor)
            .extendOnAxis(1)
            .build();

    // Part 1 (sfc): datextime=12/IC{3}->4 chunks, param=3/IC{3}->1 chunk
    // Part 2 (ml):  datextime=12/IC{3}->4 chunks, levelistxparam=60/IC{3}->20 chunks
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
