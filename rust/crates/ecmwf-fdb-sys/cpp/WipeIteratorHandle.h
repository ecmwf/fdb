// fdb WipeIteratorHandle bridge — wraps `fdb5::WipeIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::WipeIterator` for Rust FFI.
class WipeIteratorHandle {
public:

    explicit WipeIteratorHandle(fdb5::WipeIterator&& it);
    ~WipeIteratorHandle();

    WipeIteratorHandle(const WipeIteratorHandle&) = delete;
    WipeIteratorHandle& operator=(const WipeIteratorHandle&) = delete;
    WipeIteratorHandle(WipeIteratorHandle&&) = default;
    WipeIteratorHandle& operator=(WipeIteratorHandle&&) = default;

    bool hasNext();
    WipeElementData next();

private:

    fdb5::WipeIterator impl_;
    fdb5::WipeElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
