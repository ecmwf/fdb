/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/Statistics.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Statistics::Statistics():
    totalFields_(0),
    duplicates_(0),
    totalSize_(0),
    duplicatesSize_(0) {}


Statistics &Statistics::operator+=(const Statistics &rhs) {
    totalFields_ += rhs.totalFields_;
    duplicates_ += rhs.duplicates_;
    totalSize_ += rhs.totalSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}


void Statistics::report(std::ostream &out, const char *indent) const {
    out << indent << "Number of fields             : "  << eckit::BigNum(totalFields_) << std::endl;
    out << indent << "Number of bytes in data files: "  << eckit::BigNum(totalSize_) << " (" << eckit::Bytes(totalSize_) << ")" << std::endl;
    out << indent << "Number of duplicated fields  : "  << eckit::BigNum(duplicates_) << std::endl;
    out << indent << "Number of bytes in duplicates: "  << eckit::BigNum(duplicatesSize_) << " (" << eckit::Bytes(duplicatesSize_) << ")" << std::endl;
}

void Statistics::print(std::ostream &out) const {
    out << "Statistics:"
        << " number of fields: "  << eckit::BigNum(totalFields_)
        << ", number of duplicates: "    << eckit::BigNum(duplicates_)
        << ", total size: "    << eckit::Bytes(totalSize_)
        << ", size of duplicates: " << eckit::Bytes(duplicatesSize_);

}

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
