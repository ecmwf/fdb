/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_FieldDetails_H
#define fdb5_FieldDetails_H

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
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

#endif
