/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

::eckit::ClassSpec FieldLocation::classSpec_ = {&Streamable::classSpec(), "FieldLocation",};

//----------------------------------------------------------------------------------------------------------------------

FieldLocation::FieldLocation() {
}

FieldLocation::FieldLocation(eckit::Length length) :
    length_(length) {

}

FieldLocation::FieldLocation(eckit::Stream& s) {
    s >> length_;
}

void FieldLocation::encode(eckit::Stream& s) const {
    s << length_;
}

FieldLocationVisitor::~FieldLocationVisitor()
{
}

void FieldLocationPrinter::operator()(const FieldLocation& location) {
    location.dump(out_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
