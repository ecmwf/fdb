#include <cstddef>
#include <string>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/IndexMapper.h"
#include "eckit/testing/Test.h"


CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access valid") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis = {{date_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index({0}, axis) == std::vector{0UL});
}

CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access out of bounds") {

    // Given

    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis = {{date_parameter}, true};

    EXPECT_THROWS(chunked_data_view::index_mapping::to_axis_parameter_index({4}, axis));
}

CASE("index_mapping | delinearize | 1 axes 2 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(0, axis) == std::vector<size_t>({0UL, 0UL}));
    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(1, axis) == std::vector<size_t>({0UL, 1UL}));
    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(2, axis) == std::vector<size_t>({0UL, 2UL}));
    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(3, axis) == std::vector<size_t>({1UL, 0UL}));
    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(7, axis) == std::vector<size_t>({2UL, 1UL}));
    EXPECT(chunked_data_view::index_mapping::to_axis_parameter_index(11, axis) == std::vector<size_t>({3UL, 2UL}));
}

CASE("index_mapping | 2 axes 2/1 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};
    std::vector<std::string> steps = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    // std::vector<std::string> params = {"v"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};
    chunked_data_view::Parameter steps_parameter = {"steps", steps};

    const std::vector<chunked_data_view::Axis> axes = {{{date_parameter, time_parameter}, true},
                                                       {{steps_parameter}, true}};


    EXPECT(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0}, axes) == 0);
    EXPECT(chunked_data_view::index_mapping::axis_index_to_buffer_index({3, 0}, axes) == 0);
}

CASE("index_mapping | 3 axes 2/1 param | Chunked/Non-Chunked| Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};
    std::vector<std::string> steps = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    std::vector<std::string> params = {"u", "v", "w", "x", "y"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};
    chunked_data_view::Parameter steps_parameter = {"steps", steps};
    chunked_data_view::Parameter params_parameter = {"param", params};

    const std::vector<chunked_data_view::Axis> axes = {
        {{date_parameter, time_parameter}, true}, {{steps_parameter}, false}, {{params_parameter}, true}};


    EXPECT(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0, 0}, axes) == 0);
    EXPECT(chunked_data_view::index_mapping::axis_index_to_buffer_index({3, 0, 0}, axes) == 0);
}


int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
