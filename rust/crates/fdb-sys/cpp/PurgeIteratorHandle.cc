// fdb PurgeIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "PurgeIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

#include <sstream>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

PurgeIteratorHandle::PurgeIteratorHandle(fdb5::PurgeIterator&& it) : impl_(std::move(it)) {}

PurgeIteratorHandle::~PurgeIteratorHandle() = default;

bool PurgeIteratorHandle::hasNext() {
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

PurgeElementData PurgeIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    PurgeElementData data;
    std::ostringstream ss;
    ss << current_;
    data.content = rust::String(ss.str());
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
