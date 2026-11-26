// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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
