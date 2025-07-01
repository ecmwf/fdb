/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Axis.h"

#include <numeric>
#include <utility>

namespace chunked_data_view {

namespace {
template <typename Params>
size_t combinedSize(const Params& params) {
    return std::accumulate(std::begin(params), std::end(params), 1,
                           [](auto acc, const auto& p) { return acc * p.values().size(); });
}
}  // namespace

//======================================================================================================================
// class Parameter
//======================================================================================================================

Parameter::Parameter(std::tuple<const std::string, const std::vector<std::string>> tuple) : _internal(tuple) {};
Parameter::Parameter(const std::string name, const std::vector<std::string> values) : _internal(std::make_tuple(name, values)) {};


//======================================================================================================================
// class Axis
//======================================================================================================================

Axis::Axis(std::vector<Parameter> parameters, bool chunked) :
    parameters_(std::move(parameters)), size_(combinedSize(parameters_)), chunked_(chunked) {}


}  // namespace chunked_data_view
