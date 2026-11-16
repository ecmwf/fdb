// fdb StatsIteratorHandle bridge — wraps `fdb5::StatsIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/StatsIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::StatsIterator` for Rust FFI.
class StatsIteratorHandle {
public:

    explicit StatsIteratorHandle(fdb5::StatsIterator&& it);
    ~StatsIteratorHandle();

    StatsIteratorHandle(const StatsIteratorHandle&) = delete;
    StatsIteratorHandle& operator=(const StatsIteratorHandle&) = delete;
    StatsIteratorHandle(StatsIteratorHandle&&) = default;
    StatsIteratorHandle& operator=(StatsIteratorHandle&&) = default;

    bool hasNext();
    StatsElementData next();

private:

    fdb5::StatsIterator impl_;
    fdb5::StatsElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
