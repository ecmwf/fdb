
#include <cstddef>

#include "chunked_data_view/Buffer.h"

namespace chunked_data_view {

void Buffer::setBits(size_t offset, size_t length) {
    assert(offset + length <= bitset_.size());

    for (size_t i = offset; i < offset + length; ++i) {
        bitset_[i] = true;
    }
}
bool Buffer::filled() const {
    for (auto i : bitset_) {
        if (!i) {
            return false;
        }
    }
    return true;
}
};  // namespace chunked_data_view
