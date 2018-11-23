/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/WipeIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WipeElement::WipeElement(eckit::Stream& s) {
    s >> owner;
    s >> metadataPaths;
    s >> dataPaths;
    s >> otherPaths;
}

// This routine returns a very crude size (which will be rounded to page size)
// to allow a guessing of the eckit::buffer size needed to encode this object
// into a stream

size_t WipeElement::guessEncodedSize() const {

    // Sizes to use for a guess (include arbitrary size for stream-internal sizes)

    const size_t objTag = 32;

    size_t totalSize = objTag;

    // Owner
    totalSize += objTag + owner.size();

    // metadataPaths
    totalSize += objTag;
    for (const auto& p : metadataPaths) { totalSize += objTag + p.size(); }

    // dataPaths
    totalSize += objTag;
    for (const auto& p : dataPaths) { totalSize += objTag + p.size(); }

    // otherPaths
    totalSize += objTag;
    for (const auto& p : otherPaths) { totalSize += objTag + p.size(); }

    return totalSize;
}

void WipeElement::encode(eckit::Stream &s) const {
    s << owner;
    s << metadataPaths;
    s << dataPaths;
    s << otherPaths;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

