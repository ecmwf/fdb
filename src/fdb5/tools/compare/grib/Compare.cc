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

#include "eckit/exception/Exceptions.h"
#include "fdb5/api/helpers/ListElement.h"
#include "metkit/codes/api/CodesAPI.h"


#include <iomanip>
#include <iostream>

namespace compare::grib {

using namespace eckit;


CompareResult gribCompareBitExact(const fdb5::ListElement& gribLocRef, const fdb5::ListElement& gribLocTest,
                                  const Options& o, const uint8_t* bufferRef, const uint8_t* bufferTest) {
    if (o.verbose) {
        eckit::Log::info() << "[LOG] Memory comparison (Bytestream)" << std::endl;
    }
    if (gribLocTest.length() != gribLocRef.length()) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH: BITEXACT: HEADER ] Different header lenghts. Test: " << gribLocTest.length()
            << " Ref: " << gribLocRef.length() << " for " << gribLocRef;
        eckit::Log::info() << oss.str() << std::endl;
        return CompareResult::OtherMismatch;
    }

    // Bitexact comparison directly on the Bytestream begin
    auto res = bitComparison(bufferRef, bufferTest, gribLocTest.length());
    if (res == CompareResult::OtherMismatch) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH: BITEXACT: HEADER ] {Location,offset,length} = " << gribLocRef;
        eckit::Log::info() << oss.str() << std::endl;
    }
    return res;
}


CompareResult gribCompareHash(const fdb5::ListElement& gribLocRef, const fdb5::ListElement& gribLocTest,
                              const Options& o, const metkit::codes::CodesHandle& hRef,
                              const metkit::codes::CodesHandle& hTest) {
    if (!compareHeader(hRef, hTest)) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH] HEADER SECTION MD5SUMS ] Headers don't match Reference = " << gribLocRef
            << " Test = " << gribLocTest << "\n";
        eckit::Log::info() << oss.str() << std::endl;

        return CompareResult::OtherMismatch;
    }

    return compareMd5sums(hRef, hTest);
}


CompareResult gribCompareKeyByKey(const fdb5::ListElement& gribLocRef, const fdb5::ListElement& gribLocTest,
                                  const Options& o, const metkit::codes::CodesHandle& hRef,
                                  const metkit::codes::CodesHandle& hTest) {
    auto res = compareKeyByKey(hRef, hTest, o.gribKeysIgnore, o.gribKeysSelect);
    if (res == CompareResult::OtherMismatch) {
        std::ostringstream oss;
        oss << "[GRIB COMPARISON MISMATCH KEY-BY-KEY ] Grib Message REF: " << gribLocRef
            << "  TEST at {Location,offset,length} = " << gribLocTest << "\n";
        eckit::Log::info() << oss.str() << std::endl;
        return CompareResult::OtherMismatch;
    }
    return res;
}


Result gribCompareSingleMessage(const fdb5::ListElement& gribLocRef, const fdb5::ListElement& gribLocTest,
                                const Options& o) {
    Result res;
    res.match = true;

    std::unique_ptr<uint8_t[]> bufferRef  = extractGribMessage(gribLocRef);
    std::unique_ptr<uint8_t[]> bufferTest = extractGribMessage(gribLocTest);

    auto hRef  = metkit::codes::codesHandleFromMessage({bufferRef.get(), static_cast<size_t>(gribLocRef.length())});
    auto hTest = metkit::codes::codesHandleFromMessage({bufferTest.get(), static_cast<size_t>(gribLocTest.length())});

    // Non-numeric comparisons
    CompareResult methodRes = ([&]() {
        switch (o.method) {
            case Method::BitIdentical:
                return gribCompareBitExact(gribLocRef, gribLocTest, o, bufferRef.get(), bufferTest.get());
            case Method::Hash:
                return gribCompareHash(gribLocRef, gribLocTest, o, *hRef.get(), *hTest.get());
            case Method::KeyByKey:
                return gribCompareKeyByKey(gribLocRef, gribLocTest, o, *hRef.get(), *hTest.get());
            default:
                return CompareResult::OtherMismatch;
        }
    })();

    res.match = methodRes == CompareResult::Match;

    // Numeric comparison
    if (methodRes == CompareResult::DataSectionMismatch && o.scope == Scope::All) {
        auto numRes = compareDataSection(*hRef.get(), *hTest.get(), o.tolerance);

        res.relativeError = numRes.relativeError;
        res.absoluteError = numRes.absoluteError;

        if ((res.relativeError && res.relativeError->avg() > o.tolerance) ||
            (res.absoluteError && res.absoluteError->avg() > o.tolerance)) {
            std::ostringstream oss;
            oss << "[GRIB COMPARISON FAILED FP-DATA CHECK] Difference in Grib message Grib Message failed  Ref "
                   "{Location,offset,length} = "
                << gribLocRef << "  TEST at {Location,offset,length} = " << gribLocTest << "\n";
            oss << "Difference in Grib message: Absolute Error = " << res.absoluteError->avg()
                << " relative Error = " << res.relativeError->avg() << "\n";
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
    int count        = 0;

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
