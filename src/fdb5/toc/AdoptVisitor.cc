/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"

#include "fdb5/database/Catalogue.h"
#include "fdb5/toc/AdoptVisitor.h"
#include "fdb5/toc/TocEngine.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AdoptVisitor::AdoptVisitor(Archiver& owner, const Key& initialFieldKey, const PathName& path, Offset offset,
                           Length length) :
    BaseArchiveVisitor(owner, initialFieldKey), path_(path), offset_(offset), length_(length) {
    ASSERT(offset_ >= Offset(0));
    ASSERT(length_ > Length(0));
}

bool AdoptVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {
    checkMissingKeys(fullKey);

    auto cat = catalogue();
    ASSERT(cat);

    if (cat->type() == TocEngine::typeName()) {
        cat->index(datumKey, eckit::URI("file", path_), offset_, length_);
        return true;
    }
    return false;
}

void AdoptVisitor::print(std::ostream& out) const {
    out << "AdoptVisitor["
        << "path=" << path_ << ",offset=" << offset_ << ",length=" << length_ << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
