#include <tuple>
#include "chunked_data_view/Axis.h"
#include "chunked_data_view/RequestManipulation.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "metkit/mars/MarsRequest.h"

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

    for (std::size_t i = 0; i < axis.parameters().size(); ++i) {
        auto request_copy = request;


        chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, i);

        // Then
        auto values = request_copy["date"];

        EXPECT_EQUAL(values.size(), 10);
        EXPECT(values == std::get<1>(axis.parameters()[0])[i]);
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

    for (std::size_t i = 0; i < dates.size(); ++i) {

        for (std::size_t j = 0; j < times.size(); ++j) {
            auto request_copy = request;

            chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, i * times.size() + j);

            // Then
            auto date_values = request_copy["date"];
            auto time_values = request_copy["time"];

            EXPECT_EQUAL(date_values.size(), 10);
            EXPECT(date_values == dates[i]);

            std::cout << "Expecting: " << time_values << " == " << times[j] << std::endl;
            std::cout << request_copy << std::endl;

            EXPECT(time_values == times[j]);
        }
    }
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

    chunked_data_view::Axis axis({{"date", dates}, {"time", times}, {"step", steps}, {"parm", params}}, true);

    // When

    for (std::size_t i = 0; i < dates.size(); ++i) {

        for (std::size_t j = 0; j < times.size(); ++j) {

            for (std::size_t k = 0; k < steps.size(); ++k) {
                auto request_copy = request;
                auto chunk_index = k + j * steps.size() + i * (times.size() * steps.size());

                chunked_data_view::RequestManipulation::updateRequest(request_copy, axis, chunk_index);

                // Then
                auto date_values = request_copy["date"];
                auto time_values = request_copy["time"];
                auto step_values = request_copy["step"];
                auto params_values = request_copy["param"];

                EXPECT_EQUAL(date_values.size(), 10);

                std::cout << "Chunk Index: " << chunk_index << std::endl;
                std::cout << "Expecting: " << date_values << " == " << dates[i] << " | (i, j, k)=("<< i<< "," << j << "," << k <<")" << std::endl;
                EXPECT(date_values == dates[i]);
                std::cout << "Expecting: " << time_values << " == " << times[j] << " | (i, j, k)=("<< i<< "," << j << "," << k <<")" << std::endl;
                EXPECT(time_values == times[j]);
                std::cout << "Expecting: " << step_values << " == " << steps[k] << " | (i, j, k)=("<< i<< "," << j << "," << k <<")" << std::endl;
                EXPECT(step_values == steps[k]);
                std::cout << "Expecting: " << params_values << " == " << "132" << std::endl;
                EXPECT(params_values == "132");
            }
        }
    }
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
