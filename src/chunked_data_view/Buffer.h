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

#include <cassert>
#include <cstddef>
#include <vector>

namespace chunked_data_view {

class Buffer {
    std::vector<float> data_;
    std::vector<bool> bitset_;

public:

    Buffer() {};

    Buffer(const std::vector<size_t>& chunkShape) {
        size_t prod = 1;

        for (size_t i = 0; i < chunkShape.size() - 1; ++i) {
            prod *= chunkShape[i];
        }

        bitset_ = std::vector<bool>(prod);
        data_   = std::vector<float>(prod * chunkShape[chunkShape.size() - 1]);
    };

    void resize(size_t size) {
        data_.resize(size);
        bitset_.resize(size);
    }

    size_t size() const { return data_.size(); }

    size_t sizeOfValueType() const { return sizeof(decltype(data_)::value_type); }

    auto dataPtr() { return data_.data(); }

    std::vector<float>& values() { return data_; }

    void setBits(size_t index);

    void resetBits() { bitset_ = std::vector<bool>(bitset_.size()); };

    bool filled() const;
};
}  // namespace chunked_data_view
