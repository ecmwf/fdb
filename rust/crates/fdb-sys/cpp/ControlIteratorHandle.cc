// fdb ControlIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "ControlIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

ControlIteratorHandle::ControlIteratorHandle(fdb5::ControlIterator&& it) : impl_(std::move(it)) {}

ControlIteratorHandle::~ControlIteratorHandle() = default;

bool ControlIteratorHandle::hasNext() {
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

ControlElementData ControlIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    ControlElementData data;
    data.location = rust::String(current_.location.asString());
    for (const auto& id : current_.controlIdentifiers) {
        data.identifiers.push_back(id);
    }
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
