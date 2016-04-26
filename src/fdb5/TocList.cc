/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/TocList.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

TocList::TocList(const eckit::PathName &dir) : TocHandler(dir) {
    openForRead();

    TocRecord r;

    while ( readNext(r) )
        toc_.push_back(r);

    close();
}

const std::vector<TocRecord> &TocList::list() {
    return toc_;
}

void TocList::list(std::ostream &out) const {
    for ( TocVec::const_iterator itr = toc_.begin(); itr != toc_.end(); ++itr ) {
        const TocRecord &r = *itr;
        switch (r.head_.tag_) {
        case TOC_INIT:
            out << "TOC_INIT " << r << std::endl;
            break;

        case TOC_INDEX:
            out << "TOC_INDEX " << r << std::endl;
            break;

        case TOC_CLEAR:
            out << "TOC_CLEAR " << r << std::endl;
            break;

        case TOC_WIPE:
            out << "TOC_WIPE " << r << std::endl;
            break;

        default:
            throw eckit::SeriousBug("Unknown tag in TocRecord", Here());
            break;
        }
    }
}


//-----------------------------------------------------------------------------

} // namespace fdb5
