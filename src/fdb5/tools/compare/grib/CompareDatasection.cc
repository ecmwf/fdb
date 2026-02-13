/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "CompareDatasection.h"

#include "eckit/exception/Exceptions.h"
#include "fdb5/tools/compare/common/Types.h"

#include <cmath>

using namespace eckit;

namespace compare::grib {

//---------------------------------------------------------------------------------------------------------------------

NumericResult compareDataSection(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest,
                                 double tolerance) {
    NumericResult res;
    size_t valuesLenRef  = hRef.size("values");
    size_t valuesLenTest = hTest.size("values");
    double maxError      = 0.;

    if (valuesLenRef != valuesLenTest) {
        std::string errorMsg = std::string("Number of data entries does not match Reference number") +
                               std::to_string(valuesLenRef) + " test Grib number " + std::to_string(valuesLenTest);
        throw BadValue(errorMsg, Here());
    }

    auto valuesRef  = hRef.getDoubleArray("values");
    auto valuesTest = hTest.getDoubleArray("values");

    double error = 0.0;
    double denominator;
    for (size_t i = 0; i < valuesLenRef; ++i) {
        error = std::fabs(valuesRef[i] - valuesTest[i]);
        if (error <= tolerance) {
            res.absoluteError.update(0.0);
            res.relativeError.update(0.0);
        }
        else {
            res.absoluteError.update(error);
            denominator = std::fabs(valuesRef[i]) > tolerance ? std::fabs(valuesRef[i]) : tolerance;
            res.relativeError.update(error / denominator);
        }
    }

    return res;
}

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib
