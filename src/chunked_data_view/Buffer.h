#pragma once

#include <cassert>
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

    void setBits(size_t offset, size_t length);

    void resetBits() { bitset_ = std::vector<bool>(data_.size());};

    bool filled() const;
};
}  // namespace chunked_data_view
