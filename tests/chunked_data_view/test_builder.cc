#include <memory>

#include "eckit/io/MemoryHandle.h"
#include "eckit/testing/Test.h"

#include "chunked_data_view/ChunkedDataViewBuilder.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

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

CASE("ChunkedDataView | Single part - no extension axis") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();

    cdv::ChunkedDataViewBuilder(
        std::make_unique<MockFdb>(
            [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
            [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                    std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                });
            }))
        .addPart(keys,
                 {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                  cdv::AxisDefinition{{"param"}, true}},
                 std::make_unique<FakeExtractor>())
        .build();
}

CASE("ChunkedDataView | Two parts - no extension axis | Expected FAILURE") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();

    EXPECT_THROWS(cdv::ChunkedDataViewBuilder(
                      std::make_unique<MockFdb>(
                          [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                          [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                              return std::make_unique<MockListIterator>(
                                  std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                                      std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                                  });
                          }))
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .build());
}

CASE("ChunkedDataView | One part - extension axis set") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();

    cdv::ChunkedDataViewBuilder(
        std::make_unique<MockFdb>(
            [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
            [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                    std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                });
            }))
        .addPart(keys,
                 {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                  cdv::AxisDefinition{{"param"}, true}},
                 std::make_unique<FakeExtractor>())
        .extendOnAxis(1)
        .build();
}


CASE("ChunkedDataView | One part - extension axis set | Invalid extension axis index for single part") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();

    cdv::ChunkedDataViewBuilder(
        std::make_unique<MockFdb>(
            [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
            [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                return std::make_unique<MockListIterator>(std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                    std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                });
            }))
        .addPart(keys,
                 {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                  cdv::AxisDefinition{{"param"}, true}},
                 std::make_unique<FakeExtractor>())
        .extendOnAxis(2)
        .build();
}

CASE(
    "ChunkedDataView | Two parts - extension axis set | Invalid extension axis index for multiple part | Expected "
    "FAILURE") {
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();

    EXPECT_THROWS(cdv::ChunkedDataViewBuilder(
                      std::make_unique<MockFdb>(
                          [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                          [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                              return std::make_unique<MockListIterator>(
                                  std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                                      std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                                  });
                          }))
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .addPart(keys,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .extendOnAxis(3)
                      .build());
}

CASE("ChunkedDataView | Two parts - extension axis set to 0 | Mismatching parameter dimensions | Expected FAILURE") {
    const std::string keys_part_1{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    const std::string keys_part_2{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u/w,"
        "time=0/6/12/18"};

    const auto request = fdb5::FDBToolRequest::requestsFromString(keys_part_1).at(0).request();

    EXPECT_THROWS(cdv::ChunkedDataViewBuilder(
                      std::make_unique<MockFdb>(
                          [](auto& _) { return makeHandle({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); },
                          [](auto& _) -> std::unique_ptr<chunked_data_view::ListIteratorInterface> {
                              return std::make_unique<MockListIterator>(
                                  std::vector<std::tuple<fdb5::Key, std::vector<double>>>{
                                      std::make_tuple(fdb5::Key(), std::vector<double>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                                  });
                          }))
                      .addPart(keys_part_1,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .addPart(keys_part_2,
                               {cdv::AxisDefinition{{"date"}, true}, cdv::AxisDefinition{{"time"}, true},
                                cdv::AxisDefinition{{"param"}, true}},
                               std::make_unique<FakeExtractor>())
                      .extendOnAxis(0)
                      .build());
}


int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
