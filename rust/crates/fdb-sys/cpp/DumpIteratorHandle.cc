// fdb DumpIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "DumpIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

DumpIteratorHandle::DumpIteratorHandle(fdb5::DumpIterator&& it) : impl_(std::move(it)) {}

DumpIteratorHandle::~DumpIteratorHandle() = default;

bool DumpIteratorHandle::hasNext() {
    if (exhausted_) {
        return false;
    }
    if (has_current_) {
        return true;
    }

    if (impl_.next(current_)) {
        has_current_ = true;
        return true;
    }
    exhausted_ = true;
    return false;
}

DumpElementData DumpIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    DumpElementData data;
    data.content = rust::String(current_);
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
