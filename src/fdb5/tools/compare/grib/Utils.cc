/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "eckit/exception/Exceptions.h"
#include "eckit/io/DataHandle.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/FieldLocation.h"


#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace compare::grib {


const std::unordered_set<std::string>& mutableDataKeys() {
    // Set of keys that are changing if the data values change.
    // Depending on the packing type different key words are relevant.
    static const std::unordered_set<std::string> s = {
        "values",
        "codedValues",
        "packedValues",
        "referenceValue",      // simple packing, ccsds
        "binaryScaleFactor",   // simple packing, ccsds
        "decimalScaleFactor",  // simple packing, ccsds
        "bitsPerValue",        // simple packing, ccsds
        "numberOfValues",      // simple packing
        "numberOfDatapoints",  // simple packing
        "ccsdsFlags",          // ccsds
        "ccsdsBlockSize",      // ccsds
        "ccsdsRsi",            // ccsds
        "bitMapIndicator",     // bitmap
        "bitmap"               // bitmap
    };
    return s;
}

bool isDataKey(const std::string& key) {
    return mutableDataKeys().find(key) != mutableDataKeys().end();
}

void appendDataRelevantKeys(std::unordered_set<std::string>& set) {
    set.insert(mutableDataKeys().begin(), mutableDataKeys().end());
}


// Translate MARS → GRIB keys (returns vector in case of 1:N mapping)
std::vector<std::string> translateFromMARS(const std::string& marsKey) {
    static const std::unordered_map<std::string, std::vector<std::string>> mapping{
        {{"class", {"marsClass"}},
         {"expver", {"experimentVersionNumber"}},
         {"date", {"year", "month", "day"}},
         {"time", {"hour", "minute", "second"}}}};

    auto it = mapping.find(marsKey);
    if (it != mapping.end()) {
        return it->second;
    }
    return {marsKey};
}

std::unique_ptr<uint8_t[]> extractGribMessage(const fdb5::ListElement& gribLoc) {
    const auto length                 = gribLoc.length();
    std::unique_ptr<uint8_t[]> buffer = std::make_unique<uint8_t[]>(length);

    auto dh = std::unique_ptr<eckit::DataHandle>(gribLoc.location().dataHandle());

    dh->openForRead();

    // For a tco79 this seems to be the bottleneck: Optimisation suggestions: would be to keep the files open in a
    // Filestream cache. Potentially with an option to only store a certain amount of filestreams to avoid memory of
    // open file distriptor issues.
    if (!dh->read(reinterpret_cast<char*>(buffer.get()), length)) {
        throw eckit::ReadError("Could not read the data from file", Here());
    }

    return buffer;
}


}  // namespace compare::grib
