// fdb ControlIteratorHandle bridge — wraps `fdb5::ControlIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::ControlIterator` for Rust FFI.
class ControlIteratorHandle {
public:

    explicit ControlIteratorHandle(fdb5::ControlIterator&& it);
    ~ControlIteratorHandle();

    ControlIteratorHandle(const ControlIteratorHandle&) = delete;
    ControlIteratorHandle& operator=(const ControlIteratorHandle&) = delete;
    ControlIteratorHandle(ControlIteratorHandle&&) = default;
    ControlIteratorHandle& operator=(ControlIteratorHandle&&) = default;

    bool hasNext();
    ControlElementData next();

private:

    fdb5::ControlIterator impl_;
    fdb5::ControlElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
