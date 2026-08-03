#include <cstddef>
#include <string>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/mapping/IndexMapper.h"
#include "eckit/testing/Test.h"
#include "fdb5/database/Key.h"


CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access valid") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis = {{date_parameter}};

    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index({0}, axis), std::vector{0UL});
}

CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access out of bounds") {

    // Given

    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis = {{date_parameter}};

    EXPECT_THROWS(chunked_data_view::index_mapping::to_axis_parameter_index({4}, axis));
}

CASE("index_mapping | delinearize | 1 axes 2 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter}};

    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(0, axis), std::vector<size_t>({0UL, 0UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(1, axis), std::vector<size_t>({0UL, 1UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(2, axis), std::vector<size_t>({0UL, 2UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(3, axis), std::vector<size_t>({1UL, 0UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(7, axis), std::vector<size_t>({2UL, 1UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(11, axis), std::vector<size_t>({3UL, 2UL}));
}

CASE("index_mapping | computeBufferIndex | Invalid access") {

    // Given
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const std::vector<chunked_data_view::Axis> axes = {chunked_data_view::Axis({date_parameter}),
                                                       chunked_data_view::Axis({time_parameter})};
    const fdb5::Key key_01_0 = fdb5::Key::parse(
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01,"
        "levtype=sfc,"
        "param=v,"
        "time=0");

    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {5, 3}, {5, 3}), 0));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {5, 3}, {0, 3}), 0));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {5, 3}, {5, 0}), 0));
    EXPECT_NO_THROW((chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {4, 2}, {5, 3}), 0));
}

CASE("index_mapping | computeBufferIndex | 1 axes 2 param | Valid access / Sliding window") {

    // Given
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const std::vector<chunked_data_view::Axis> axes = {chunked_data_view::Axis({date_parameter}),
                                                       chunked_data_view::Axis({time_parameter})};

    for (size_t k = 0; k < dates.size(); ++k) {

        for (size_t l = 0; l < times.size(); ++l) {

            const auto bufferOffset = std::vector<size_t>{k, l};
            const auto bufferExtent = std::vector<size_t>{5, 3};

            for (size_t i = 0; i < dates.size(); ++i) {

                for (size_t j = 0; j < times.size(); ++j) {

                    std::string req_str =
                        "type=an,"
                        "domain=g,"
                        "expver=0001,"
                        "stream=oper,"
                        "levtype=sfc,"
                        "param=v,";

                    req_str += "date=" + dates[i] + ",";
                    req_str += "time=" + times[j];

                    const fdb5::Key key = fdb5::Key::parse(req_str);

                    std::cout << "i: " << i << " | j: " << j << " | k:" << k << " | l: " << l << std::endl;
                    if (k + i < bufferExtent[0] && j + l < bufferExtent[1]) {
                        EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key, {0, 0},
                                                                                          bufferOffset, bufferExtent),
                                     times.size() * (k + i) + (j + l));
                    }
                    else {
                        EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key, {0, 0},
                                                                                            bufferOffset, bufferExtent),
                                       times.size() * (k + i) + (j + l)));
                    }
                }
            }
        }
    }
}

CASE("index_mapping | computeBufferIndex | 1 axes 2 param | Valid access") {

    // Given
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const std::vector<chunked_data_view::Axis> axes = {chunked_data_view::Axis({date_parameter}),
                                                       chunked_data_view::Axis({time_parameter})};
    const fdb5::Key key_01_0 = fdb5::Key::parse(
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01,"
        "levtype=sfc,"
        "param=v,"
        "time=0");

    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {0, 0}, {5, 3}), 0);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {0, 1}, {5, 3}), 1);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {0, 2}, {5, 3}), 2);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {1, 0}, {5, 3}), 3);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {1, 1}, {5, 3}), 4);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {1, 2}, {5, 3}), 5);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {2, 0}, {5, 3}), 6);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {3, 0}, {5, 3}), 9);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {4, 0}, {5, 3}), 12);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {4, 1}, {5, 3}), 13);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_01_0, {0, 0}, {4, 2}, {5, 3}), 14);

    const fdb5::Key key_02_12 = fdb5::Key::parse(
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-02,"
        "levtype=sfc,"
        "param=v,"
        "time=12");

    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {0, 0}, {5, 3}), 5);
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {0, 1}, {5, 3}), 6));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {0, 2}, {5, 3}), 7));

    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {1, 0}, {5, 3}), 8);
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {1, 1}, {5, 3}), 9));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {1, 2}, {5, 3}), 10));

    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {2, 0}, {5, 3}), 11);
    EXPECT_EQUAL(chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {3, 0}, {5, 3}), 14);

    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {4, 0}, {5, 3}), 15));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {4, 1}, {5, 3}), 16));
    EXPECT_THROWS((chunked_data_view::index_mapping::computeBufferIndex(axes, key_02_12, {0, 0}, {4, 2}, {5, 3}), 17));
}


int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
