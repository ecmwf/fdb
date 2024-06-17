/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "Compare.h"

#include "CompareBitwise.h"
#include "CompareDatasection.h"
#include "CompareHash.h"
#include "CompareKeys.h"
#include "Utils.h"

#include "fdb5/api/helpers/ListElement.h"
#include "metkit/codes/api/CodesAPI.h"


#include <iomanip>
#include <iostream>

namespace compare::grib {

using namespace eckit;


CompareResult gribCompareBitExact(const fdb5::ListElement& gribLoc1, const fdb5::ListElement& gribLoc2,
                                  const Options& o, const uint8_t* buffer1, const uint8_t* buffer2) {
    if (o.verbose) {
        eckit::Log::info() << "[LOG] Memory comparison (Bytestream)" << std::endl;
    }
    if (gribLoc2.length() != gribLoc1.length()) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH: BITEXACT: HEADER ] Different header lengths. FDB2: " << gribLoc2.length()
            << " FDB1: " << gribLoc1.length() << " for " << gribLoc1;
        eckit::Log::info() << oss.str() << std::endl;
        return CompareResult::OtherMismatch;
    }

    // Bitexact comparison directly on the Bytestream begin
    auto res = bitComparison(buffer1, buffer2, gribLoc2.length());
    if (res == CompareResult::OtherMismatch) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH: BITEXACT: HEADER ] {Location,offset,length} = " << gribLoc1;
        eckit::Log::info() << oss.str() << std::endl;
    }
    return res;
}


CompareResult gribCompareHash(const fdb5::ListElement& gribLoc1, const fdb5::ListElement& gribLoc2, const Options& o,
                              const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2) {
    if (!compareHeader(h1, h2)) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH] HEADER SECTION MD5SUMS ] Headers don't match FDB1 = " << gribLoc1
            << " FDB2 = " << gribLoc2 << "\n";
        eckit::Log::info() << oss.str() << std::endl;

        return CompareResult::OtherMismatch;
    }

    return compareMd5sums(h1, h2);
}


CompareResult gribCompareKeyByKey(const fdb5::ListElement& gribLoc1, const fdb5::ListElement& gribLoc2,
                                  const Options& o, const metkit::codes::CodesHandle& h1,
                                  const metkit::codes::CodesHandle& h2) {
    auto res = compareKeyByKey(h1, h2, o.gribKeysIgnore, o.gribKeysSelect);
    if (res == CompareResult::OtherMismatch) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH KEY-BY-KEY ] Grib Message FDB1: " << gribLoc1
            << "  FDB2 at {Location,offset,length} = " << gribLoc2 << "\n";
        eckit::Log::info() << oss.str() << std::endl;
        return CompareResult::OtherMismatch;
    }
    return res;
}


Result gribCompareSingleMessage(const fdb5::ListElement& gribLoc1, const fdb5::ListElement& gribLoc2,
                                const Options& o) {
    Result res;
    res.match = true;

    std::unique_ptr<uint8_t[]> buffer1 = extractGribMessage(gribLoc1);
    std::unique_ptr<uint8_t[]> buffer2 = extractGribMessage(gribLoc2);

    auto h1 = metkit::codes::codesHandleFromMessage({buffer1.get(), static_cast<size_t>(gribLoc1.length())});
    auto h2 = metkit::codes::codesHandleFromMessage({buffer2.get(), static_cast<size_t>(gribLoc2.length())});

    // Non-numeric comparisons
    CompareResult methodRes = ([&]() {
        switch (o.method) {
            case Method::BitIdentical:
                return gribCompareBitExact(gribLoc1, gribLoc2, o, buffer1.get(), buffer2.get());
            case Method::Hash:
                return gribCompareHash(gribLoc1, gribLoc2, o, *h1.get(), *h2.get());
            case Method::KeyByKey:
                return gribCompareKeyByKey(gribLoc1, gribLoc2, o, *h1.get(), *h2.get());
            default:
                return CompareResult::OtherMismatch;
        }
    })();

    res.match = methodRes == CompareResult::Match;

    // Numeric comparison
    if (methodRes == CompareResult::DataSectionMismatch && o.scope == Scope::All) {
        auto numRes = compareDataSection(*h1.get(), *h2.get(), o.tolerance);

        res.relativeError = numRes.relativeError;
        res.absoluteError = numRes.absoluteError;

        if ((res.relativeError && res.relativeError->max > o.tolerance) ||
            (res.absoluteError && res.absoluteError->max > o.tolerance)) {
            std::ostringstream oss;
            oss << "[GRIB COMPARISON FAILED FP-DATA CHECK] Grib Message FP-DATA check failed for FDB1 "
                   "{Location,offset,length} = "
                << gribLoc1 << "  FDB2 at {Location,offset,length} = " << gribLoc2 << "\n";
            oss << "Difference in Grib message: Absolute Error (max) = " << res.absoluteError->max
                << " Relative Error (max) = " << res.relativeError->max << "\n";
            eckit::Log::info() << oss.str() << std::endl;
            res.match = false;
        }
        else {
            res.match = true;
        }
    }
    return res;
}


void printProgressBar(std::ostream& os, std::size_t current, std::size_t total, std::size_t bar_width = 30) {
    if (total == 0)
        return;

    double ratio       = static_cast<double>(current) / total;
    std::size_t filled = static_cast<std::size_t>(ratio * bar_width);

    os << '\r' << '[';

    for (std::size_t i = 0; i < bar_width; ++i) {
        if (i < filled)
            os << '#';
        else
            os << '-';
    }

    os << "] " << current << '/' << total << " (" << std::fixed << std::setprecision(0) << ratio * 100 << "%)"
       << std::flush;
}


Result compareGrib(const DataIndex& ref, const DataIndex& test, const Options& opts) {
    Result res;

    const auto total = ref.size();
    size_t count     = 0;

    for (auto& [key, value] : ref) {
        ++count;
        if (opts.verbose) {
            printProgressBar(eckit::Log::info(), count, total);
        }

        try {
            auto modKey         = applyKeyDiff(key, opts.marsReqDiff);
            const auto& testVal = test.at(modKey);

            Result singleRes = gribCompareSingleMessage(value, testVal, opts);
            res.update(singleRes);

            if (!singleRes.match) {
                eckit::Log::info() << "[GRIB COMPARISON MISMATCH] Grib Comparison failed for MARS key " << key
                                   << std::endl;
            }
        }
        catch (...) {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH] Grib Comparison failed for MARS key" << key << std::endl;
            throw;
        }
    }

    if (opts.verbose) {
        eckit::Log::info() << std::endl;
    }

    return res;
}

}  // namespace compare::grib
