/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/toc/AdoptVisitor.h"
#include "fdb5/toc/TocDBWriter.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AdoptVisitor::AdoptVisitor(Archiver &owner, const Key &field, const PathName &path, Offset offset, Length length) :
    BaseArchiveVisitor(owner, field),
    path_(path),
    offset_(offset),
    length_(length) {
    ASSERT(offset_ >= Offset(0));
    ASSERT(length_ > Length(0));
}

bool AdoptVisitor::selectDatum(const Key &key, const Key &full) {

    // Log::info() << "selectDatum " << key << ", " << full << " " << length_ << std::endl;
    checkMissingKeys(full);

    ASSERT(current());

    TocDBWriter *writer = dynamic_cast<TocDBWriter *>(current());

    ASSERT(writer);

    writer->index(key, path_, offset_, length_);

    return true;
}

void AdoptVisitor::print(std::ostream &out) const {
    out << "AdoptVisitor["
        << "path=" << path_
        << ",offset=" << offset_
        << ",length=" << length_
        << "]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
