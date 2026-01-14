#include <algorithm>
#include <cstddef>
#include <ostream>
#include <string>
#include <vector>
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/RequestManipulation.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "metkit/mars/MarsRequest.h"

template <class BidirIt>
bool next_perm(BidirIt first, BidirIt last) {
    auto r_first = std::make_reverse_iterator(last);
    auto r_last  = std::make_reverse_iterator(first);
    auto left    = std::is_sorted_until(r_first, r_last);

    if (left != r_last) {
        auto right = std::upper_bound(r_first, left, *left);
        std::iter_swap(left, right);
    }

    std::reverse(left.base(), last);
    return left != r_last;
}


CASE("RequestManipulation | Axis test single axis for Indices | Can create a subindex") {

    // Given
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    auto request = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
    chunked_data_view::Axis axis({{"date", {"2020-01-01", "2020-01-02", "2020-01-03"}}}, true);

    // When

    for (size_t i = 0; i < axis.parameters().size(); ++i) {
        auto request_copy = request;


        chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, i);

        // Then
        auto values = request_copy["date"];

        EXPECT_EQUAL(values.size(), 10);
        EXPECT(values == axis.parameters()[0].values()[i]);
    }
}

CASE("RequestManipulation | Axis test multiple axis for Indices | Can create a subindex") {

    // Given
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v/u,"
        "time=0/6/12/18"};

    auto request                   = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    chunked_data_view::Axis axis({{"date", dates}, {"time", times}}, true);

    // When

    for (size_t i = 0; i < dates.size(); ++i) {

        for (size_t j = 0; j < times.size(); ++j) {
            auto request_copy = request;

            chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, i * times.size() + j);

            // Then
            auto date_values = request_copy["date"];
            auto time_values = request_copy["time"];

            EXPECT_EQUAL(date_values.size(), 10);
            EXPECT(date_values == dates[i]);

            eckit::Log::debug() << "Expecting: " << time_values << " == " << times[j] << std::endl;
            eckit::Log::debug() << request_copy << std::endl;

            EXPECT(time_values == times[j]);
        }
    }
}


bool assert_arrays(
    const metkit::mars::MarsRequest& request,
    const chunked_data_view::Axis&
        axis) {  // chunked_data_view::Axis::Parameter first, chunked_data_view::Axis::Parameter second,
                 // chunked_data_view::Axis::Parameter third, chunked_data_view::Axis::Parameter fourth) {

    EXPECT(axis.parameters().size() == 4);

    const std::string first_name                = axis.parameters()[0].name();
    const std::vector<std::string> first_values = axis.parameters()[0].values();

    const std::string second_name                = axis.parameters()[1].name();
    const std::vector<std::string> second_values = axis.parameters()[1].values();

    const std::string third_name                = axis.parameters()[2].name();
    const std::vector<std::string> third_values = axis.parameters()[2].values();

    const std::string fourth_name                = axis.parameters()[3].name();
    const std::vector<std::string> fourth_values = axis.parameters()[3].values();


    for (size_t i = 0; i < first_values.size(); ++i) {

        for (size_t j = 0; j < second_values.size(); ++j) {

            for (size_t k = 0; k < third_values.size(); ++k) {

                for (size_t l = 0; l < fourth_values.size(); ++l) {

                    auto request_copy = request;
                    auto chunk_index = l + k * fourth_values.size() + j * (fourth_values.size() * third_values.size()) +
                                       i * (fourth_values.size() * third_values.size() * second_values.size());

                    chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, chunk_index);

                    // Then
                    auto first_result_values  = request_copy[first_name];
                    auto second_result_values = request_copy[second_name];
                    auto third_result_values  = request_copy[third_name];
                    auto fourth_result_values = request_copy[fourth_name];

                    eckit::Log::debug() << "Chunk Index: " << chunk_index << std::endl;
                    eckit::Log::debug() << "Expecting: " << first_result_values << " == " << first_values[i]
                                        << " | (i, j, k, l)=(" << i << "," << j << "," << k << "," << l << ")"
                                        << std::endl;
                    EXPECT(first_result_values == first_values[i]);
                    eckit::Log::debug() << "Expecting: " << second_result_values << " == " << second_values[j]
                                        << " | (i, j, k, l)=(" << i << "," << j << "," << k << "," << l << ")"
                                        << std::endl;
                    EXPECT(second_result_values == second_values[j]);
                    eckit::Log::debug() << "Expecting: " << third_result_values << " == " << third_values[k]
                                        << " | (i, j, k, l)=(" << i << "," << j << "," << k << "," << l << ")"
                                        << std::endl;
                    EXPECT(third_result_values == third_values[k]);
                    eckit::Log::debug() << "Expecting: " << fourth_result_values << " == " << fourth_values[l]
                                        << " | (i, j, k, l)=(" << i << "," << j << "," << k << "," << l << ")"
                                        << std::endl;
                    EXPECT(fourth_result_values == fourth_values[l]);
                }
            }
        }
    }

    return true;
}

CASE("RequestManipulation | Axis test multiple axis for Indices 2 | Can handle mixed axis") {

    // Given
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v,"
        "step=0/1/2/3/4/5/6/7/8/9/10/11/12,"
        "time=0/6/12/18"};

    auto request                    = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
    std::vector<std::string> dates  = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times  = {"0", "6", "12", "18"};
    std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    std::vector<std::string> params = {"v"};

    chunked_data_view::Parameter date_parameter  = {"date", dates};
    chunked_data_view::Parameter time_parameter  = {"time", times};
    chunked_data_view::Parameter step_parameter  = {"step", steps};
    chunked_data_view::Parameter param_parameter = {"param", params};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter, step_parameter, param_parameter}, true};

    EXPECT(assert_arrays(request, axis));
}

CASE("RequestManipulation | Axis test multiple axis | Non-chunked") {

    // Given
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v,"
        "step=0/1/2/3/4/5/6/7/8/9/10/11/12,"
        "time=0/6/12/18"};

    auto request                    = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
    std::vector<std::string> dates  = {"20200101", "20200102", "20200103", "20200104"};
    std::vector<std::string> times  = {"0000", "0600", "1200", "1800"};
    std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    std::vector<std::string> params = {"v"};

    chunked_data_view::Parameter date_parameter  = {"date", dates};
    chunked_data_view::Parameter time_parameter  = {"time", times};
    chunked_data_view::Parameter step_parameter  = {"step", steps};
    chunked_data_view::Parameter param_parameter = {"param", params};

    const chunked_data_view::Axis axis = {{date_parameter, time_parameter, step_parameter, param_parameter}, false};

    auto request_copy = request;
    EXPECT_NO_THROW(chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, 0));

    for (size_t i = 1; i < dates.size() * times.size() * steps.size() * params.size(); ++i) {
        auto request_copy = request;
        EXPECT_THROWS(chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, i));
    }
}

CASE("RequestManipulation | Axis test multiple axis for Indices | Permutations") {

    // Given
    const std::string keys{
        "type=an,"
        "domain=g,"
        "expver=0001,"
        "stream=oper,"
        "date=2020-01-01/to/2020-01-04,"
        "levtype=sfc,"
        "param=v,"
        "step=0/1/2/3/4/5/6/7/8/9/10/11/12,"
        "time=0/6/12/18"};

    const auto request                    = fdb5::FDBToolRequest::requestsFromString(keys).at(0).request();
    const std::vector<std::string> dates  = {"2020-01-01", "2020-01-02", "2020-01-03"};
    const std::vector<std::string> times  = {"0", "6", "12", "18"};
    const std::vector<std::string> steps  = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
    const std::vector<std::string> params = {"v"};

    const chunked_data_view::Parameter date_parameter  = {"date", dates};
    const chunked_data_view::Parameter time_parameter  = {"time", times};
    const chunked_data_view::Parameter step_parameter  = {"step", steps};
    const chunked_data_view::Parameter param_parameter = {"param", params};

    const std::vector<chunked_data_view::Parameter> param_vector = {date_parameter, time_parameter, step_parameter,
                                                                    param_parameter};

    std::vector<size_t> perm = {0, 1, 2, 3};

    do {

        auto& first  = param_vector[perm[0]];
        auto& second = param_vector[perm[1]];
        auto& third  = param_vector[perm[2]];
        auto& fourth = param_vector[perm[3]];

        const chunked_data_view::Axis axis = {{first, second, third, fourth}, true};

        eckit::Log::debug() << "Current permutation: (" << perm[0] << ", " << perm[1] << ", " << perm[2] << ", "
                            << perm[3] << ") | ";
        eckit::Log::debug() << "Current order: (" << param_vector[perm[0]].name() << ", "
                            << param_vector[perm[1]].name() << ", " << param_vector[perm[2]].name() << ", "
                            << param_vector[perm[3]].name() << ") " << std::endl;
        EXPECT(assert_arrays(request, axis));

    } while (next_perm(perm.begin(), perm.end()));
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
