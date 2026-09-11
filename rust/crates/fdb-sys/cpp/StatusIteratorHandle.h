// fdb StatusIteratorHandle bridge — wraps `fdb5::StatusIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/StatusIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::StatusIterator` for Rust FFI.
class StatusIteratorHandle {
public:

    explicit StatusIteratorHandle(fdb5::StatusIterator&& it);
    ~StatusIteratorHandle();

    StatusIteratorHandle(const StatusIteratorHandle&) = delete;
    StatusIteratorHandle& operator=(const StatusIteratorHandle&) = delete;
    StatusIteratorHandle(StatusIteratorHandle&&) = default;
    StatusIteratorHandle& operator=(StatusIteratorHandle&&) = default;

    bool hasNext();
    StatusElementData next();

private:

    fdb5::StatusIterator impl_;
    fdb5::StatusElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
