/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/FieldDetails.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void FieldDetails::print(std::ostream& out) const {
    out << "(referenceValue=" << referenceValue_ << ",binaryScaleFactor=" << binaryScaleFactor_
        << ",decimalScaleFactor=" << decimalScaleFactor_ << ",bitsPerValue=" << bitsPerValue_
        << ",offsetBeforeData=" << offsetBeforeData_ << ",offsetBeforeBitmap=" << offsetBeforeBitmap_
        << ",numberOfValues=" << numberOfValues_ << ",numberOfDataPoints=" << numberOfDataPoints_
        << ",sphericalHarmonics=" << sphericalHarmonics_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
