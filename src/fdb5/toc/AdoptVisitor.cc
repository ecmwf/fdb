// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
