/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "CompareKeys.h"

#include "eckit/log/Log.h"
#include "eckit/types/Types.h"  // Include stringification through operator<<
#include "fdb5/tools/compare/grib/CompareBitwise.h"
#include "metkit/codes/api/CodesTypes.h"

#include <algorithm>
#include <string>
#include <type_traits>
#include <vector>

using namespace eckit;

namespace compare::grib {

namespace {

// Use SFINA avoid conversions which results in operator<< ambiguity
template <typename Stream, typename Type,
          std::enable_if_t<std::is_same_v<std::decay_t<Type>, metkit::codes::CodesValue>, bool> = true>
Stream& operator<<(Stream& os, const Type& val) {
    std::visit([&](const auto& v) -> void { os << v; }, val);
    return os;
}

template <typename Stream>
Stream& operator<<(Stream& os, const metkit::codes::NativeType& t) {
    switch (t) {
        case metkit::codes::NativeType::String: {
            os << "STRING";
            return os;
        }
        case metkit::codes::NativeType::Double: {
            os << "DOUBLE";
            return os;
        }
        case metkit::codes::NativeType::Long: {
            os << "LONG";
            return os;
        }
        case metkit::codes::NativeType::Label: {
            os << "LABEL";
            return os;
        }
        case metkit::codes::NativeType::Section: {
            os << "SECTION";
            return os;
        }
        case metkit::codes::NativeType::Bytes: {
            os << "BYTES";
            return os;
        }
        case metkit::codes::NativeType::Missing: {
            os << "MISSING";
            return os;
        }
        case metkit::codes::NativeType::Undefined: {
            os << "UNDEFINED";
            return os;
        }
    }
    return os;
}

template <typename>
struct IsVector : std::false_type {};

template <typename T, typename A>
struct IsVector<std::vector<T, A>> : std::true_type {};

template <typename T>
inline constexpr bool IsVector_v = IsVector<T>::value;

bool holdsVector(const metkit::codes::CodesValue& val) {
    return std::visit([&](const auto& v) -> bool { return IsVector_v<std::decay_t<decltype(v)>>; }, val);
}

int64_t vectorLength(const metkit::codes::CodesValue& val) {
    return std::visit(
        [&](const auto& v) -> int64_t {
            if constexpr (IsVector_v<std::decay_t<decltype(v)>>) {
                return v.size();
            };
            return 1;
        },
        val);
}

}  // namespace


// only compare the key of each map. the tupel is expected to diverge. first as bytestreams and only if they don't match
// get more details
CompareResult compareValues(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest,
                            const std::string& name) {
    if (hRef.isMissing(name) && hTest.isMissing(name)) {
        return CompareResult::Match;
    }

    if (hRef.isMissing(name)) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                           << " is set to missing in reference FDB but is not missing in test FDB" << std::endl;
        return CompareResult::OtherMismatch;
    }

    if (hTest.isMissing(name)) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                           << " is set to missing in test FDB but is not missing in reference FDB" << std::endl;
        return CompareResult::OtherMismatch;
    }


    auto typeRef  = hRef.type(name);
    auto typeTest = hTest.type(name);

    auto valRef  = hRef.get(name);
    auto valTest = hTest.get(name);
    if (typeRef != typeTest) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name << "Grib comparison Type mismatch: Reference Value "
                           << valRef << " CODES_TYPE " << typeRef << "\nTest value: " << valTest << " CODES_TYPE "
                           << typeTest << std::endl;
        return CompareResult::OtherMismatch;
    }


    if (holdsVector(valRef)) {
        if (holdsVector(valTest)) {
            auto l1 = vectorLength(valRef);
            auto l2 = vectorLength(valTest);
            if (l1 != l2) {
                eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << " Key" << name
                                   << " contains an array with different sizes "
                                   << " lengthRef= " << l1 << ", lengthTest= " << l2 << std::endl;
                return CompareResult::OtherMismatch;
            }
        }
        else {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                               << "Grib comparison Type mismatch: Reference Value is a vector " << valRef
                               << " \nTest value is no vector: " << valTest << std::endl;
            return CompareResult::OtherMismatch;
        }
    }
    else {
        if (holdsVector(valTest)) {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                               << "Grib comparison Type mismatch: Reference Value is no vector " << valRef
                               << " \nTest value is vector: " << valTest << std::endl;
            return CompareResult::OtherMismatch;
        }
    }


    if (valTest != valRef) {
        static const std::unordered_set<std::string> floatingValueFields{{"values", "packedValues", "codedValues"}};
        if (floatingValueFields.find(name) != floatingValueFields.end()) {
            // Return true and indicate that a check with a tolerance boundary needs to be made
            return CompareResult::DataSectionMismatch;
        }
        else {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << " Key=" << name << ", Reference Value=" << valRef
                               << ", Test value:=" << valTest << std::endl;
            return CompareResult::OtherMismatch;
        }
    }

    return CompareResult::Match;
}


CompareResult compareKeyByKey(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest,
                              const std::unordered_set<std::string>& ignore_list,
                              const std::unordered_set<std::string>& match_list) {

    CompareResult res = CompareResult::Match;

    auto updateRes = [&](CompareResult singleResult) {
        switch (singleResult) {
            case CompareResult::OtherMismatch: {
                // If something mismatches, the overall result is always a missmatch
                res = CompareResult::OtherMismatch;
                return;
            }
            case CompareResult::DataSectionMismatch: {
                // Previously everything matched, now something in the datasection is not completely equal
                if (res == CompareResult::Match) {
                    res = CompareResult::DataSectionMismatch;
                }
                return;
            }
            case CompareResult::Match: {
                // Do nothing - no update required, if always matched it will stay matched
                return;
            }
        }
    };

    bool matchreturn       = true;
    bool dataSectionreturn = true;
    if (match_list.size() != 0) {
        for (auto key : match_list) {
            updateRes(compareValues(hRef, hTest, key));
        }
    }
    else {
        for (const auto& keyIt : hRef.keys(metkit::codes::KeyIteratorFlags::SkipComputed)) {
            auto name = keyIt.name();

            if (ignore_list.find(name) != ignore_list.end()) {
                continue;
            }

            updateRes(compareValues(hRef, hTest, name));
        }
    }
    return res;
}

}  // namespace compare::grib
