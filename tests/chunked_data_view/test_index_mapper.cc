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

    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index({0}, axis), std::vector{0UL});
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

    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(0, axis), std::vector<size_t>({0UL, 0UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(1, axis), std::vector<size_t>({0UL, 1UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(2, axis), std::vector<size_t>({0UL, 2UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(3, axis), std::vector<size_t>({1UL, 0UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(7, axis), std::vector<size_t>({2UL, 1UL}));
    EXPECT_EQUAL(chunked_data_view::index_mapping::to_axis_parameter_index(11, axis), std::vector<size_t>({3UL, 2UL}));
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


    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0}, axes), 0);
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({3, 0}, axes), 0);
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


    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0, 0}, axes), 0);
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({3, 0, 0}, axes), 0);
}


CASE("index_mapping | axis_index_to_buffer_index | extension axis remaps stride and offset") {

    // 3 unchunked axes: [A(size=3), B(size=2, extension), C(size=4)]
    // Without extension: flat index = a * 2*4 + b * 4 + c = a*8 + b*4 + c
    // With extension (combinedExtSize=5, offset=2):
    //   flat index = a * 5*4 + (b+2) * 4 + c = a*20 + (b+2)*4 + c

    std::vector<std::string> a_vals = {"a0", "a1", "a2"};
    std::vector<std::string> b_vals = {"b0", "b1"};
    std::vector<std::string> c_vals = {"c0", "c1", "c2", "c3"};

    chunked_data_view::Parameter a_param = {"a", a_vals};
    chunked_data_view::Parameter b_param = {"b", b_vals};
    chunked_data_view::Parameter c_param = {"c", c_vals};

    const std::vector<chunked_data_view::Axis> axes = {
        {{a_param}, false}, {{b_param}, false}, {{c_param}, false}};

    // Without extension params: a=1, b=1, c=2 → 1*8 + 1*4 + 2 = 14
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({1, 1, 2}, axes), 14);

    // With extension on axis 1 (combinedExtSize=5, offset=2):
    // a=1, b=1, c=2 → 1*20 + (1+2)*4 + 2 = 20 + 12 + 2 = 34
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({1, 1, 2}, axes, 1, 5, 2), 34);

    // a=0, b=0, c=0 with offset=2 → 0*20 + (0+2)*4 + 0 = 8
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0, 0}, axes, 1, 5, 2), 8);

    // a=0, b=0, c=0 with offset=0 → 0 (same as no extension)
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 0, 0}, axes, 1, 5, 0), 0);
}

CASE("index_mapping | axis_index_to_buffer_index | extension axis with chunked neighbours") {

    // Axes: [A(chunked), B(unchunked, size=3, extension), C(unchunked, size=4)]
    // Chunked axes are skipped in index computation.
    // Without extension: flat = b * 4 + c
    // With extension (combinedExtSize=7, offset=3): flat = (b+3) * 4 + c

    std::vector<std::string> a_vals = {"a0", "a1"};
    std::vector<std::string> b_vals = {"b0", "b1", "b2"};
    std::vector<std::string> c_vals = {"c0", "c1", "c2", "c3"};

    chunked_data_view::Parameter a_param = {"a", a_vals};
    chunked_data_view::Parameter b_param = {"b", b_vals};
    chunked_data_view::Parameter c_param = {"c", c_vals};

    const std::vector<chunked_data_view::Axis> axes = {
        {{a_param}, true}, {{b_param}, false}, {{c_param}, false}};

    // Without extension: a=0 (chunked, ignored), b=2, c=3 → 2*4 + 3 = 11
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 2, 3}, axes), 11);

    // With extension on axis 1 (combinedExtSize=7, offset=3):
    // b=2, c=3 → (2+3)*4 + 3 = 23
    EXPECT_EQUAL(chunked_data_view::index_mapping::axis_index_to_buffer_index({0, 2, 3}, axes, 1, 7, 3), 23);
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
