/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Utils.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   Feb 2026

#pragma once

#include "fdb5/api/helpers/ListElement.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace compare::grib {

enum class CompareResult {
    Match,
    DataSectionMismatch,
    OtherMismatch,
};

/// @param key
/// @return Check if a given key is a data key
bool isDataKey(const std::string& key);

/// @param set Output set in which additional data relevant keys are inserted.
/// @return Append all data keys to the provided set
void appendDataRelevantKeys(std::unordered_set<std::string>& set);

/// Translates a mars keys to list of equivalent grib keys.
/// Might also return just one element or an empty list.
/// @param marsKey Mars key to translate
/// @return Vector with translated key names
std::vector<std::string> translateFromMARS(const std::string& marsKey);

/// @param gribLoc Information on a specific message to extract
/// @return Smart pointing owning buffer of the grib message
std::unique_ptr<uint8_t[]> extractGribMessage(const fdb5::ListElement& gribLoc);

}  // namespace compare::grib
