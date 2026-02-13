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

using namespace compare;

namespace compare::mars {

/// Compare the mars keys of two messages from different locations.
///
/// @param ref  Reference location of the message
/// @param test Test location of the message
/// @param opts Additional options - notably marsReqDiff is used to lookup and compare requests
/// @return Comparison result. As mars is not comparing data, only matched is expected to be set
Result compareMarsKeys(const DataIndex& ref, const DataIndex& test, const Options& opts);

}  // namespace compare::mars
