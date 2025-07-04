
#include <cstddef>

#include "chunked_data_view/Buffer.h"

namespace chunked_data_view {

void Buffer::setBits(size_t offset, size_t length) {
    assert(offset + length <= bitset_.size());

    for (size_t i = offset; i < length; ++i) {
        bitset_[i + offset] = true;
    }
}
bool Buffer::filled() const {
    return std::all_of(bitset_.begin(), bitset_.end(), [](bool v) {return v;});
}
};  // namespace chunked_data_view
