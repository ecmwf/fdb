// fdb WipeIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "WipeIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

#include <sstream>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

WipeIteratorHandle::WipeIteratorHandle(fdb5::WipeIterator&& it) : impl_(std::move(it)) {}

WipeIteratorHandle::~WipeIteratorHandle() = default;

bool WipeIteratorHandle::hasNext() {
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

WipeElementData WipeIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    WipeElementData data;
    std::ostringstream ss;
    ss << current_;
    data.content = rust::String(ss.str());
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
