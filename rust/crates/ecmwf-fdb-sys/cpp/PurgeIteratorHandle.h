// fdb PurgeIteratorHandle bridge — wraps `fdb5::PurgeIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/PurgeIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::PurgeIterator` for Rust FFI.
class PurgeIteratorHandle {
public:

    explicit PurgeIteratorHandle(fdb5::PurgeIterator&& it);
    ~PurgeIteratorHandle();

    PurgeIteratorHandle(const PurgeIteratorHandle&) = delete;
    PurgeIteratorHandle& operator=(const PurgeIteratorHandle&) = delete;
    PurgeIteratorHandle(PurgeIteratorHandle&&) = default;
    PurgeIteratorHandle& operator=(PurgeIteratorHandle&&) = default;

    bool hasNext();
    PurgeElementData next();

private:

    fdb5::PurgeIterator impl_;
    fdb5::PurgeElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
