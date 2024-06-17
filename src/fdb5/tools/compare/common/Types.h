/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Types.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   Feb 2026

#pragma once
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"

#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace compare {

//---------------------------------------------------------------------------------------------------------------------

/// A pair describing differences between two key-value pairs in a requests, e.g. expver=1234 or expver=2345
/// Each value is optional - to indicate if a key is missing in one the comparing pairs.
using ValueDiff = std::pair<std::optional<std::string>, std::optional<std::string>>;

/// Describes a difference between two requests, e.g. {expver={1234,2345}}
using KeyDiffMap = std::map<std::string, ValueDiff>;

/// Set of unique keys
using KeySet = std::unordered_set<std::string>;

}  // namespace compare

namespace compare {

/// Creates a map of full specificed FDB keys (as a KeyValueMap),
/// mapping to a data location which describes where the message can be loaded from.
using DataIndex = std::unordered_map<fdb5::Key, fdb5::ListElement>;

/// Runs a request on a fdb to create a list of filtered entries to compared on.
///
/// @param fdb FDB on which to operate on
/// @param req Request which performs a selection on the fdb
/// @param ignore Map of key-value pairs to ignore
/// @return filtered DataIndex
DataIndex assembleCompareMap(fdb5::FDB& fdb, const fdb5::FDBToolRequest& req, const fdb5::Key& ignore);

//---------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& os, const KeyDiffMap& km);

std::ostream& operator<<(std::ostream& os, const KeySet& km);

std::ostream& operator<<(std::ostream& os, const DataIndex& idx);


//---------------------------------------------------------------------------------------------------------------------

// void parseKeyValues(KeyValueMap& container, const std::string& keyValueStr);
// KeyValueMap parseKeyValues(const std::string& keyValueStr);

void parseKeySet(KeySet& container, const std::string& keyValueStr);
KeySet parseKeySet(const std::string& keyValueStr);

compare::KeyDiffMap requestDiff(const fdb5::Key& l, const fdb5::Key& r);


/// @param km Map of keys with values
/// @param diff Map of keys with differences
/// @param swapPair Optional - If false the second pair of the difference is put in the resulting request.
///                 If true the first one is used.
/// @return New KeyValueMap with differences applied
fdb5::Key applyKeyDiff(fdb5::Key km, const KeyDiffMap& diff, bool swapPair = false);


//---------------------------------------------------------------------------------------------------------------------

/// Comparison Scope
enum class Scope {
    Mars,        // Compare MARS keys only
    HeaderOnly,  // Compare MARS and Grib Header of the message
    All          // Compare full message, including data section
};

Scope parseScope(const std::string& s);
std::ostream& operator<<(std::ostream& os, const Scope& scope);

// Comparison methods for GRIB comparison
enum class Method {
    KeyByKey,      // Compare value of metadata keys (default)
    BitIdentical,  // Messages must be bit identical - fast
    Hash           // Use sections md5 hashes
};

Method parseMethod(const std::string& s);
std::ostream& operator<<(std::ostream& os, const Method& method);


struct Options {
    Scope scope   = Scope::Mars;
    Method method = Method::KeyByKey;

    // For MARS comparison:
    fdb5::Key ignoreMarsKeys;

    // Expected differences between two requets (e.g. comparing different expver)
    KeyDiffMap marsReqDiff;

    // Optional explicit requests to compare different subtrees, e.g. expver=1111 with expver=2222
    std::optional<fdb5::Key> referenceRequest;
    std::optional<fdb5::Key> testRequest;

    // List of specific grib keys which are used for comparison only
    KeySet gribKeysSelect;

    // List of specific grib keys which are ignored for comparison only.
    // Used if gribKeysSelect is empty.
    KeySet gribKeysIgnore;

    // Additional Options for Grib comparator
    double tolerance = 0.0;

    // Control output granularity
    bool verbose = false;
};


//---------------------------------------------------------------------------------------------------------------------

struct NumericError {
    double sum   = 0.0;
    double min   = 0.0;
    double max   = 0.0;
    size_t count = 0;

    double avg() const;

    void update(double val);
    void update(const NumericError& other);
};

std::ostream& operator<<(std::ostream&, const NumericError&);


struct Result {
    bool match = true;  // success
    std::optional<NumericError> relativeError;
    std::optional<NumericError> absoluteError;

    void update(const Result& other);
};

std::ostream& operator<<(std::ostream&, const Result&);

//---------------------------------------------------------------------------------------------------------------------


}  // namespace compare
