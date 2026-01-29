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

#include "eckit/exception/Exceptions.h"
#include "fdb5/database/Key.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <numeric>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

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
Parameter::Parameter(const std::string name, const std::vector<std::string> values) :
    _internal(std::make_tuple(name, values)) {};


//======================================================================================================================
// class Axis
//======================================================================================================================

Axis::Axis(std::vector<Parameter> parameters, bool chunked) :
    parameters_(std::move(parameters)), size_(combinedSize(parameters_)), chunked_(chunked) {}

size_t Axis::index(const fdb5::Key& key) const {
    // Sanity check whether the key is containing information about the axis
    const std::vector<std::string>& keys = key.names();

    size_t prod  = 1;
    size_t index = 0;

    for (int i = parameters().size() - 1; i >= 0; --i) {

        const auto& param = parameters()[i];

        auto [it, success] = key.find(param.name());

        if (!success) {
            throw eckit::Exception("Couldn't find the parameter name in the keys of the request.");
        }

        // Find the index of the key value in the axis
        auto res = std::find(std::begin(param.values()), std::end(param.values()), it->second);

        if (res == param.values().end()) {
            throw eckit::Exception("Couldn't request's key value in the axis.");
        }

        size_t value_index = std::distance(param.values().begin(), res);

        index += value_index * prod;
        prod *= param.values().size();
    }

    return index;
}


}  // namespace chunked_data_view
