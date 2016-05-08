/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/IndexStatistics.h"

// #include "eckit/log/Bytes.h"
// #include "eckit/log/Plural.h"

// #include "fdb5/toc/TocIndex.h"
// #include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

IndexStatistics::IndexStatistics():
    fields_(0),
    duplicates_(0),
    fieldsSize_(0),
    duplicatesSize_(0) {}


IndexStatistics &IndexStatistics::operator+=(const IndexStatistics &rhs) {
    fields_ += rhs.fields_;
    duplicates_ += rhs.duplicates_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}


void IndexStatistics::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Fields", fields_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicates_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
}

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
