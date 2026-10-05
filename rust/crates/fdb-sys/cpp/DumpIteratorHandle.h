// fdb DumpIteratorHandle bridge — wraps `fdb5::DumpIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/DumpIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::DumpIterator` for Rust FFI.
class DumpIteratorHandle {
public:

    explicit DumpIteratorHandle(fdb5::DumpIterator&& it);
    ~DumpIteratorHandle();

    DumpIteratorHandle(const DumpIteratorHandle&) = delete;
    DumpIteratorHandle& operator=(const DumpIteratorHandle&) = delete;
    DumpIteratorHandle(DumpIteratorHandle&&) = default;
    DumpIteratorHandle& operator=(DumpIteratorHandle&&) = default;

    bool hasNext();
    DumpElementData next();

private:

    fdb5::DumpIterator impl_;
    fdb5::DumpElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
