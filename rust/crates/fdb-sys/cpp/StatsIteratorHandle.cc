// fdb StatsIteratorHandle bridge — implementation.

#include "fdb_exceptions.h"

#include "StatsIteratorHandle.h"
#include "fdb-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

#include <sstream>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

StatsIteratorHandle::StatsIteratorHandle(fdb5::StatsIterator&& it) : impl_(std::move(it)) {}

StatsIteratorHandle::~StatsIteratorHandle() = default;

bool StatsIteratorHandle::hasNext() {
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

StatsElementData StatsIteratorHandle::next() {
    if (!has_current_ && !hasNext()) {
        throw eckit::OutOfRange("Iterator exhausted", Here());
    }

    has_current_ = false;

    StatsElementData data;
    data.index_statistics.fields_count = current_.indexStatistics.fieldsCount();
    data.index_statistics.fields_size = current_.indexStatistics.fieldsSize();
    data.index_statistics.duplicates_count = current_.indexStatistics.duplicatesCount();
    data.index_statistics.duplicates_size = current_.indexStatistics.duplicatesSize();
    {
        std::ostringstream os;
        current_.indexStatistics.report(os);
        data.index_statistics.report = os.str();
    }
    {
        std::ostringstream os;
        current_.dbStatistics.report(os);
        data.db_statistics.report = os.str();
    }
    return data;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge
