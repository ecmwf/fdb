
#include <cstddef>

#include "chunked_data_view/Buffer.h"

namespace chunked_data_view {

void Buffer::setBits(size_t index) {
    assert(index < bitset_.size());
    bitset_[index] = true;
}
bool Buffer::filled() const {
    return std::all_of(bitset_.begin(), bitset_.end(), [](bool v) {return v;});
}
};  // namespace chunked_data_view
