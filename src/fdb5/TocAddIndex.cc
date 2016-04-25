/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/TocAddIndex.h"
#include "fdb5/Key.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

TocAddIndex::TocAddIndex(const eckit::PathName &dir, const eckit::PathName &idx, const Key &key) :
    TocHandler( dir ),
    index_(idx),
    tocMD_(key.valuesToString()) {
}

TocAddIndex::~TocAddIndex() {
    openForAppend();
    TocRecord r = makeRecordIdxInsert(index_, tocMD_);
    append(r);
    close();
}

eckit::PathName TocAddIndex::index() const {
    return index_;
}


//-----------------------------------------------------------------------------

} // namespace fdb5
