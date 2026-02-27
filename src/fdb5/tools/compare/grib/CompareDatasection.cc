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

NumericResult compareDataSection(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2,
                                 double tolerance) {
    NumericResult res;
    size_t valuesLen1 = h1.size("values");
    size_t valuesLen2 = h2.size("values");
    double maxError   = 0.;

    if (valuesLen1 != valuesLen2) {
        std::string errorMsg = std::string("Number of data entries does not match 1erence number") +
                               std::to_string(valuesLen1) + " test Grib number " + std::to_string(valuesLen2);
        throw BadValue(errorMsg, Here());
    }

    auto values1 = h1.getDoubleArray("values");
    auto values2 = h2.getDoubleArray("values");

    double error = 0.0;
    double denominator;
    for (size_t i = 0; i < valuesLen1; ++i) {
        error = std::fabs(values1[i] - values2[i]);
        if (error <= tolerance) {
            res.absoluteError.update(0.0);
            res.relativeError.update(0.0);
        }
        else {
            res.absoluteError.update(error);
            denominator = std::fabs(values1[i]) > tolerance ? std::fabs(values1[i]) : tolerance;
            res.relativeError.update(error / denominator);
        }
    }

    return res;
}

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib
