/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Compare.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "fdb5/tools/compare/common/Types.h"

namespace compare::grib {

/// Compare grib messages from different locations.
/// The grib messages are compared on multiple levels.
/// First by comparing the header through a selected method (Bitexact, comparing section hashes, or key by key).
/// Then eventually also the data section is compared for equivalence.
/// To control floating-point comparison, a fp-tolerance can be customized in the options.
///
/// @param i1 Location of the FDB1 message
/// @param i2 Location of the FDB2 message
/// @param opts Additional options
/// @return Comparison result indicating whether the GRIB headers and, if requested, data sections match
Result compareGrib(const DataIndex& i1, const DataIndex& i2, const Options& opts);

}  // namespace compare::grib
