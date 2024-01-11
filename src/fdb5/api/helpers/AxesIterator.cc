/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/helpers/AxesIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AxesElement::AxesElement(Key&& dbKey, IndexAxis&& axes) :
        dbKey_(std::move(dbKey)), axes_(std::move(axes)) {}

AxesElement::AxesElement(eckit::Stream &s) {
    s >> dbKey_;
    axes_ = IndexAxis(s, IndexAxis::currentVersion());
}

void AxesElement::print(std::ostream &out, bool withLocation, bool withLength) const {
    out << "Axes(db=" << dbKey_ << ", axes=" << axes_ << ")";
}

void AxesElement::encode(eckit::Stream &s) const {
    s << dbKey_;
    axes_.encode(s, IndexAxis::currentVersion());
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
