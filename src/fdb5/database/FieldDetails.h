// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#pragma once

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/types/FixedString.h"
#include "eckit/types/Types.h"

#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct FieldDetails {

    FieldDetails() :
        referenceValue_(0),
        binaryScaleFactor_(0),
        decimalScaleFactor_(0),
        bitsPerValue_(0),
        offsetBeforeData_(0),
        offsetBeforeBitmap_(0),
        numberOfValues_(0),
        numberOfDataPoints_(0),
        sphericalHarmonics_(0) {}

    operator bool() const { return bitsPerValue_ && !gridMD5_.empty(); }

    double referenceValue_;
    long binaryScaleFactor_;
    long decimalScaleFactor_;
    unsigned long bitsPerValue_;
    unsigned long offsetBeforeData_;
    unsigned long offsetBeforeBitmap_;
    unsigned long numberOfValues_;
    unsigned long numberOfDataPoints_;
    long sphericalHarmonics_;

    eckit::FixedString<32> gridMD5_;  ///< md5 of the grid geometry section in GRIB

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const FieldDetails& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

