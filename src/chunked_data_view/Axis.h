/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once

#include "fdb5/database/Key.h"

#include <cstddef>
#include <string>
#include <tuple>
#include <vector>

namespace chunked_data_view {

class Parameter {

public:

    Parameter(std::tuple<const std::string, const std::vector<std::string>> tuple);
    Parameter(const std::string name, const std::vector<std::string> values);

    const std::string& name() const { return std::get<0>(_internal); }
    const std::vector<std::string>& values() const { return std::get<1>(_internal); }

private:

    std::tuple<const std::string, const std::vector<std::string>> _internal;
};

class Axis {
public:

    Axis(std::vector<chunked_data_view::Parameter> parameters, bool chunked);

    size_t size() const { return size_; }

    bool contains(const std::string& key) {
        for (const auto& param : parameters_) {
            if (param.name() == key) {
                return true;
            }
        }

        return false;
    }

    bool isChunked() const { return chunked_; }
    const std::vector<chunked_data_view::Parameter>& parameters() const { return parameters_; }

    size_t index(const fdb5::Key& key) const;

private:

    std::vector<chunked_data_view::Parameter> parameters_{};
    size_t size_{};
    bool chunked_{};
};

}  // namespace chunked_data_view
