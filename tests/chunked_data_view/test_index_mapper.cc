#include "IndexMapper.h"
#include "chunked_data_view/Axis.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

CASE("index_mapping | linearize | 1 axes 1 param | Chunked | Access valid") {

    // Given
    std::vector<std::string> dates              = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis          = {{date_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::linearize({0}, axis) == 0);
}


CASE("index_mapping | linearize | 1 axes 1 param | Chunked | Access out of bounds") {

    // Given

    std::vector<std::string> dates              = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis          = {{date_parameter}, true};

    EXPECT_THROWS(chunked_data_view::index_mapping::linearize({4}, axis));
}

CASE("index_mapping | linearize | 1 axes 1 param | Chunked | Index too many dimensions") {

    // Given
    std::vector<std::string> dates              = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis          = {{date_parameter}, true};

    EXPECT_THROWS(chunked_data_view::index_mapping::linearize({0, 0}, axis));
}

CASE("index_mapping | linearize | 1 axes 2 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::linearize({0, 0}, axis) == 0);
    EXPECT(chunked_data_view::index_mapping::linearize({0, 1}, axis) == 1);
    EXPECT(chunked_data_view::index_mapping::linearize({0, 2}, axis) == 2);
    EXPECT(chunked_data_view::index_mapping::linearize({3, 2}, axis) == 11);
}

CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access valid") {

    // Given
    std::vector<std::string> dates              = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis          = {{date_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::delinearize({0}, axis) == std::vector{0UL});
}

CASE("index_mapping | delinearize | 1 axes 1 param | Chunked | Access out of bounds") {

    // Given

    std::vector<std::string> dates              = {"20200101", "20200102", "20200103", "20200104"};
    chunked_data_view::Parameter date_parameter = {"date", dates};
    const chunked_data_view::Axis axis          = {{date_parameter}, true};

    EXPECT_THROWS(chunked_data_view::index_mapping::delinearize({4}, axis));
}

CASE("index_mapping | delinearize | 1 axes 2 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};

    chunked_data_view::Parameter date_parameter = {"date", dates};
    chunked_data_view::Parameter time_parameter = {"time", times};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter}, true};

    EXPECT(chunked_data_view::index_mapping::delinearize(0, axis) == std::vector<std::size_t>({0UL, 0UL}));
    EXPECT(chunked_data_view::index_mapping::delinearize(1, axis) == std::vector<std::size_t>({0UL, 1UL}));
    EXPECT(chunked_data_view::index_mapping::delinearize(2, axis) == std::vector<std::size_t>({0UL, 2UL}));
    EXPECT(chunked_data_view::index_mapping::delinearize(3, axis) == std::vector<std::size_t>({1UL, 0UL}));
    EXPECT(chunked_data_view::index_mapping::delinearize(7, axis) == std::vector<std::size_t>({2UL, 1UL}));
    EXPECT(chunked_data_view::index_mapping::delinearize(11, axis) == std::vector<std::size_t>({3UL, 2UL}));
}

CASE("index_mapping | 2 axes 2/1 param | Chunked | Valid access") {

    // Given
    std::vector<std::string> dates = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times = {"0", "6", "12"};
    std::vector<std::string> steps = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    // std::vector<std::string> params = {"v"};

    chunked_data_view::Parameter date_parameter  = {"date", dates};
    chunked_data_view::Parameter time_parameter  = {"time", times};
    chunked_data_view::Parameter steps_parameter = {"steps", steps};

    const std::vector<chunked_data_view::Axis> axes = {{{date_parameter, time_parameter}, true},
                                                       {{steps_parameter}, true}};


    EXPECT(chunked_data_view::index_mapping::linearize({0, 0}, axes) == 0);
    EXPECT(chunked_data_view::index_mapping::linearize({3, 0}, axes) == 0);
}

CASE("index_mapping | 3 axes 2/1 param | Chunked/Non-Chunked| Valid access") {

    // Given
    std::vector<std::string> dates  = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times  = {"0", "6", "12"};
    std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    std::vector<std::string> params = {"u", "v", "w", "x", "y"};

    chunked_data_view::Parameter date_parameter   = {"date", dates};
    chunked_data_view::Parameter time_parameter   = {"time", times};
    chunked_data_view::Parameter steps_parameter  = {"steps", steps};
    chunked_data_view::Parameter params_parameter = {"param", params};

    const std::vector<chunked_data_view::Axis> axes = {
        {{date_parameter, time_parameter}, true}, {{steps_parameter}, false}, {{params_parameter}, true}};


    EXPECT(chunked_data_view::index_mapping::linearize({0, 0, 0}, axes) == 0);
    EXPECT(chunked_data_view::index_mapping::linearize({3, 0, 0}, axes) == 0);
}

// CASE("index_mapping | 4 axes 1 param | Chunked") {
//
//     // Given
//     const std::string keys{
//         "type=an,"
//         "domain=g,"
//         "expver=0001,"
//         "stream=oper,"
//         "date=2020-01-01/to/2020-01-04,"
//         "levtype=sfc,"
//         "param=v,"
//         "step=0/1/2/3/4/5/6/7/8/9/10/11/12,"
//         "time=0/6/12/18"};
//
//     auto request                    = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
//     std::vector<std::string> dates  = {"20200101", "20200102", "20200103", "20200104"};
//     std::vector<std::string> times  = {"0000", "0600", "1200", "1800"};
//     std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
//     std::vector<std::string> params = {"v"};
//
//     chunked_data_view::Parameter date_parameter  = {"date", dates};
//     chunked_data_view::Parameter time_parameter  = {"time", times};
//     chunked_data_view::Parameter step_parameter  = {"step", steps};
//     chunked_data_view::Parameter param_parameter = {"param", params};
//
//     const std::vector<chunked_data_view::Axis> axes = {{{date_parameter}, true}, {{time_parameter}, true},
//     {{step_parameter}, true}, {{param_parameter}, false}};
//
//     EXPECT(chunked_data_view::index_mapping::linearize({0, 0, 0, 0}, axes) == 0);
//     EXPECT(chunked_data_view::index_mapping::linearize({0, 0, 1, 0}, axes) == 1);
//     EXPECT(chunked_data_view::index_mapping::linearize({0, 1, 0, 0}, axes) == 13);
//     EXPECT(chunked_data_view::index_mapping::linearize({1, 0, 0, 0}, axes) == 52);
//
// }

// CASE("index_mapping | 1 axis 4 params | Non-chunked") {
//
//     // Given
//     const std::string keys{
//         "type=an,"
//         "domain=g,"
//         "expver=0001,"
//         "stream=oper,"
//         "date=2020-01-01/to/2020-01-04,"
//         "levtype=sfc,"
//         "param=v,"
//         "step=0/1/2/3/4/5/6/7/8/9/10/11/12,"
//         "time=0/6/12/18"};
//
//     auto request                    = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
//     std::vector<std::string> dates  = {"20200101", "20200102", "20200103", "20200104"};
//     std::vector<std::string> times  = {"0000", "0600", "1200", "1800"};
//     std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
//     std::vector<std::string> params = {"v"};
//
//     chunked_data_view::Parameter date_parameter  = {"date", dates};
//     chunked_data_view::Parameter time_parameter  = {"time", times};
//     chunked_data_view::Parameter step_parameter  = {"step", steps};
//     chunked_data_view::Parameter param_parameter = {"param", params};
//
//     const chunked_data_view::Axis axis = {{date_parameter, time_parameter, step_parameter, param_parameter}, false};
//
//     chunked_data_view::index_mapping::linearize({0, 0, 0, 0}, {axis});
//
// }

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
