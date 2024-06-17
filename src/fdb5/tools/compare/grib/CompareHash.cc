/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include <iostream>
#include <string>

#include <cstring>
#include <vector>

#include "eckit/exception/Exceptions.h"

#include "CompareHash.h"

using std::array;

using namespace eckit;

namespace compare::grib {

//---------------------------------------------------------------------------------------------------------------------

const std::vector<std::string>& makeGribMD5Sections(int gribEdition) {
    //"md5Headers"; This is possible to add but is already covered in memcmp the header message (But the header message
    // does not contain section data 7 (or 4 in Grib1) bitmap 6 (or 3 in Grib1) and section DRS (only present in Grib2))
    switch (gribEdition) {
        case 1: {
            // compare MD5 Keys for Grib 1 bitmap and datasection 3 and 4
            static const std::vector<std::string> md5Sec{"md5Section3", "md5Section4"};
            return md5Sec;
        }
        case 2: {
            // check MD5sums for Grib2 section 5,6 and 7
            static const std::vector<std::string> md5Sec{"md5Section5", "md5Section6", "md5Section7"};
            return md5Sec;
        }
        default: {
            static const std::vector<std::string> md5Sec{};
            return md5Sec;
        }
    }
}

CompareResult compareMd5sums(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest) {
    long gribEditionRef  = hRef.getLong("editionNumber");
    long gribEditionTest = hTest.getLong("editionNumber");

    if (gribEditionRef != gribEditionTest) {
        throw BadValue("The Grib edition numbers don't match Reference = Grib " + std::to_string(gribEditionRef) +
                           " Test = " + std::to_string(gribEditionTest),
                       Here());
    }
    if (!((gribEditionRef == 1) || (gribEditionRef == 2))) {
        throw BadValue("Currently only Grib 1 and Grib 2 are supported. The Gribnumber of these messages is: " +
                           std::to_string(gribEditionRef),
                       Here());
    }

    for (const auto& value : makeGribMD5Sections(gribEditionRef)) {
        auto md5HashValueRef  = hRef.getString(value);
        auto md5HashValueTest = hTest.getString(value);

        if ((md5HashValueRef.size() != md5HashValueTest.size())) {
            std::string errorMsg = std::string("Hashkeys need to have the same size Ref Hash ") + md5HashValueTest +
                                   " Test " + md5HashValueTest + "Section " + value;
            throw BadValue(errorMsg, Here());
        }

        if (md5HashValueRef != md5HashValueTest) {
            if ((gribEditionRef == 1 && value == "md5Section4") || (gribEditionRef == 2 && value == "md5Section7")) {
                return CompareResult::DataSectionMismatch;
            }
            return CompareResult::OtherMismatch;
        }
    }
    return CompareResult::Match;
}

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib
