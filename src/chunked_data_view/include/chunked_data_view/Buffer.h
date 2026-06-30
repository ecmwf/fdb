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

#include <cstddef>
#include <utility>
#include <vector>

namespace chunked_data_view {

class Buffer {
    std::vector<double> data_;
    std::vector<bool> bitset_;

public:

    Buffer() {};

    Buffer(std::vector<double>& data) : data_(std::move(data)) { bitset_ = std::vector<bool>(data.size()); };

    void resize(size_t size) {
        data_.resize(size);
        bitset_.resize(size);
    }

    size_t size() const { return data_.size(); }

    size_t sizeOfValueType() const { return sizeof(decltype(data_)::value_type); }

    auto dataPtr() { return data_.data(); }

    std::vector<double>& values() { return data_; }
};
}  // namespace chunked_data_view
