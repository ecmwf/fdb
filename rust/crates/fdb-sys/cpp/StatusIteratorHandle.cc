// fdb StatusIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "StatusIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

StatusIteratorHandle::StatusIteratorHandle(fdb5::StatusIterator&& it) : impl_(std::move(it)) {}

StatusIteratorHandle::~StatusIteratorHandle() = default;

bool StatusIteratorHandle::hasNext() {
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

StatusElementData StatusIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    StatusElementData data;
    data.location = rust::String(current_.location.asString());
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
