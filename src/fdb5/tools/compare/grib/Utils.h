// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

/// @param set Output set in which all data-relevant keys are inserted.
void appendDataRelevantKeys(std::unordered_set<std::string>& set);

/// Translates a mars keys to list of equivalent grib keys.
/// Might also return just one element or an empty list.
/// @param marsKey Mars key to translate
/// @return Vector with translated key names
std::vector<std::string> translateFromMARS(const std::string& marsKey);

/// @param gribLoc Information on a specific message to extract
/// @return Smart pointer owning buffer of the grib message
std::unique_ptr<uint8_t[]> extractGribMessage(const fdb5::ListElement& gribLoc);

}  // namespace compare::grib
