// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/RequestManipulation.h"
#include "chunked_data_view/exception/BoundingBoxException.h"
#include "chunked_data_view/exception/RequestManipulationException.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/helpers/FDBToolRequest.h"

CASE("RequestManipulation | Bounding Box | Dimensions mismatch Bounding Box | Error") {

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
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    std::vector<std::string> params = {"v", "u"};
    chunked_data_view::Axis datetime_axis({{"date", dates}, {"time", times}});
    chunked_data_view::Axis param_axis({{"param", params}});

    // When
    {
        chunked_data_view::PartBoundingBox bb_smaller{{0}, {0}};
        EXPECT_THROWS_AS(
            chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, bb_smaller),
            chunked_data_view::BoundingBoxException);
    }

    {
        chunked_data_view::PartBoundingBox bb_bigger{{0, 0, 0}, {0, 0, 0}};
        EXPECT_THROWS_AS(
            chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, bb_bigger),
            chunked_data_view::BoundingBoxException);
    }
}

CASE("RequestManipulation | Bounding Box | Single Point") {

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
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    std::vector<std::string> params = {"v", "u"};
    chunked_data_view::Axis datetime_axis({{"date", dates}, {"time", times}});
    chunked_data_view::Axis param_axis({{"param", params}});

    chunked_data_view::PartBoundingBox bb{{0, 0}, {0, 0}};

    // When
    auto request_copy = chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, bb);

    // Then
    std::vector<std::string> date_values{};
    request_copy.getValues("date", date_values);
    std::vector<std::string> time_values{};
    request_copy.getValues("time", time_values);
    std::vector<std::string> param_values{};
    request_copy.getValues("param", param_values);

    for (const auto& date : date_values) {
        eckit::Log::debug() << date << " " << std::endl;
    }

    for (const auto& time : time_values) {
        eckit::Log::debug() << time << " " << std::endl;
    }

    for (const auto& param : param_values) {
        eckit::Log::debug() << param << " " << std::endl;
    }

    EXPECT_EQUAL(date_values.size(), 1);
    EXPECT_EQUAL(date_values[0], "2020-01-01");
    EXPECT_EQUAL(time_values.size(), 1);
    EXPECT_EQUAL(time_values[0], "0");
    EXPECT_EQUAL(param_values.size(), 1);
    EXPECT_EQUAL(param_values[0], "v");
}

CASE("RequestManipulation | Bounding Box | Inverval") {

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
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    std::vector<std::string> params = {"v", "u"};
    chunked_data_view::Axis datetime_axis({{"date", dates}, {"time", times}});
    chunked_data_view::Axis param_axis({{"param", params}});


    {
        // When
        chunked_data_view::PartBoundingBox bb{{1, 1}, {2, 1}};
        auto request_copy =
            chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, bb);

        // Then
        std::vector<std::string> date_values{};
        request_copy.getValues("date", date_values);
        std::vector<std::string> time_values{};
        request_copy.getValues("time", time_values);
        std::vector<std::string> param_values{};
        request_copy.getValues("param", param_values);

        EXPECT_EQUAL(date_values.size(), 1);
        EXPECT_EQUAL(date_values, (std::vector<std::string>{"2020-01-01"}));
        EXPECT_EQUAL(time_values.size(), 2);
        EXPECT_EQUAL(time_values, (std::vector<std::string>{"6", "12"}));
        EXPECT_EQUAL(param_values.size(), 1);
        EXPECT_EQUAL(param_values, (std::vector<std::string>{"u"}));
    }
    {
        // When
        chunked_data_view::PartBoundingBox bb{{1, 0}, {3, 1}};
        auto request_copy =
            chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, bb);

        // Then
        std::vector<std::string> date_values{};
        request_copy.getValues("date", date_values);
        std::vector<std::string> time_values{};
        request_copy.getValues("time", time_values);
        std::vector<std::string> param_values{};
        request_copy.getValues("param", param_values);

        EXPECT_EQUAL(date_values.size(), 1);
        EXPECT_EQUAL(date_values, (std::vector<std::string>{"2020-01-01"}));
        EXPECT_EQUAL(time_values.size(), 3);
        EXPECT_EQUAL(time_values, (std::vector<std::string>{"6", "12", "18"}));
        EXPECT_EQUAL(param_values.size(), 2);
        EXPECT_EQUAL(param_values, (std::vector<std::string>{"v", "u"}));
    }
}


CASE("RequestManipulation | Bounding Box | 2D Axis | Interval | Multi MARS requests required Error") {

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
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    std::vector<std::string> params = {"v", "u"};
    chunked_data_view::Axis datetime_axis({{"date", dates}, {"time", times}});
    chunked_data_view::Axis param_axis({{"param", params}});

    // When
    EXPECT_THROWS_AS(
        (chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, {{3, 1}, {4, 1}})),
        chunked_data_view::RequestManipulationException);

    EXPECT_THROWS_AS(
        (chunked_data_view::RequestManipulation::selectRequest(request, {datetime_axis, param_axis}, {{7, 1}, {8, 1}})),
        chunked_data_view::RequestManipulationException);
}

CASE("RequestManipulation | Bounding Box | 3D Axis | Inverval | Multi MARS requests required Error") {

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
    std::vector<std::string> dates = {"2020-01-01", "2020-01-02", "2020-01-03"};
    std::vector<std::string> times = {"0", "6", "12", "18"};
    std::vector<std::string> params = {"v", "u"};
    chunked_data_view::Axis datetimeparam_axis({{"date", dates}, {"time", times}, {"param", params}});

    // When
    // Border of 2020-01-01/2020-01-02
    EXPECT_THROWS_AS((chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{7}, {8}})),
                     chunked_data_view::RequestManipulationException);
    // Border of 0/6
    EXPECT_THROWS_AS((chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{1}, {2}})),
                     chunked_data_view::RequestManipulationException);
    // Border of 6/12
    EXPECT_THROWS_AS((chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{3}, {4}})),
                     chunked_data_view::RequestManipulationException);

    EXPECT_NO_THROW((chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{0}, {3}})));
    EXPECT_NO_THROW((chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{0}, {7}})));
    EXPECT_NO_THROW(
        (chunked_data_view::RequestManipulation::selectRequest(request, {datetimeparam_axis}, {{0}, {23}})));
}

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
