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

#include <cassert>
#include <cstddef>
#include <string>
#include <tuple>
#include <variant>
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

class Chunks {

public:

    explicit Chunks(const std::vector<std::variant<size_t, std::tuple<size_t, size_t>>>& chunks) {

        for (const auto& element : chunks) {
            if (std::holds_alternative<size_t>(element)) {
                extensions_.emplace_back(std::get<size_t>(element));
            }
            else {
                auto [extent, amount] = std::get<std::tuple<size_t, size_t>>(element);
                for (size_t i = 0; i < amount; ++i) {
                    extensions_.emplace_back(extent);
                }
            }
        }
    }

    Chunks(size_t chunk_extension, size_t amount) :
        Chunks(std::vector<std::variant<size_t, std::tuple<size_t, size_t>>>{
            std::tuple<size_t, size_t>{chunk_extension, amount}}) {};

    size_t size() const { return extensions_.size(); }
    std::vector<size_t> extensions() const { return extensions_; }

private:

    std::vector<size_t> extensions_{};
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

    bool isChunked() const {
        assert(chunks_.size() > 0);
        return (chunks_.extensions().size() > 1);
    }  // TODO(TKR): Remove once every Axis is chunked

    std::vector<size_t> chunks() const { return chunks_.extensions(); }  // TODO(TKR): Remove once every Axis is chunked
    const std::vector<chunked_data_view::Parameter>& parameters() const { return parameters_; }

    size_t index(const fdb5::Key& key) const;

private:

    std::vector<chunked_data_view::Parameter> parameters_{};
    size_t size_{};
    Chunks chunks_;
};

}  // namespace chunked_data_view
