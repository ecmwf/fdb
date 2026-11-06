// fdb ListIteratorHandle bridge — wraps `fdb5::ListIterator`.
#pragma once

#include "Types.h"

#include "fdb5/api/helpers/ListIterator.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::ListIterator` for Rust FFI.
class ListIteratorHandle {
public:

    explicit ListIteratorHandle(fdb5::ListIterator&& it);
    ~ListIteratorHandle();

    ListIteratorHandle(const ListIteratorHandle&) = delete;
    ListIteratorHandle& operator=(const ListIteratorHandle&) = delete;
    ListIteratorHandle(ListIteratorHandle&&) = default;
    ListIteratorHandle& operator=(ListIteratorHandle&&) = default;

    bool hasNext();
    ListElementData next();

    /// Drain the iterator via `fdb5::ListIterator::dumpCompact`, returning the
    /// aggregated MARS-request text plus the field/byte counters.
    CompactListingData dump_compact();

    /// Access the underlying `fdb5::ListIterator` for other bridge code.
    fdb5::ListIterator& inner() { return impl_; }

private:

    fdb5::ListIterator impl_;
    fdb5::ListElement current_;
    bool has_current_ = false;
    bool exhausted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
