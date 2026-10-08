// fdb ListIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "Key.h"
#include "ListIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

#include <sstream>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

ListIteratorHandle::ListIteratorHandle(fdb5::ListIterator&& it) : impl_(std::move(it)) {}

ListIteratorHandle::~ListIteratorHandle() = default;

bool ListIteratorHandle::hasNext() {
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

ListElementData ListIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    ListElementData data;
    // `fullUri()` encodes the entry's offset in the URI fragment and its
    // length in the `length` query parameter — round-trippable through
    // `FdbHandle::read_uri` without manual seeking, mirroring upstream
    // `fdb-url` / `fdb-hammer`.
    data.uri = rust::String(current_.location().fullUri().asRawString());
    data.offset = current_.location().offset();
    data.length = current_.location().length();

    const auto& keys = current_.keys();
    if (keys.size() > 0) {
        data.db_key = Key::to_data(keys[0]);
    }
    if (keys.size() > 1) {
        data.index_key = Key::to_data(keys[1]);
    }
    if (keys.size() > 2) {
        data.datum_key = Key::to_data(keys[2]);
    }

    data.timestamp = static_cast<int64_t>(current_.timestamp());

    return data;
}

CompactListingData ListIteratorHandle::dump_compact() {
    std::ostringstream os;
    auto [fields, length] = impl_.dumpCompact(os);
    CompactListingData data;
    data.text = rust::String(os.str());
    data.fields = static_cast<uint64_t>(fields);
    data.total_bytes = static_cast<uint64_t>(length);
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
