/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   CompareDatasection.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "fdb5/tools/compare/common/Types.h"

#include "metkit/codes/api/CodesAPI.h"


namespace compare::grib {

struct NumericResult {
    compare::NumericError relativeError;
    compare::NumericError absoluteError;
};

/// Compares the data section (values) of two grib messages.
///
/// @param hRef Reference codes handle
/// @param hTest Test codes handle
/// @param tolerance floating point comparison tolerance
/// @return Numeric comparison results containing information on absolute and relative error
NumericResult compareDataSection(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest,
                                 double tolerance);

}  // namespace compare::grib
