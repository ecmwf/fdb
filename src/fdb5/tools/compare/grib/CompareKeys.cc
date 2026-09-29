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

// Use SFINAE to avoid conversions which result in operator<< ambiguity
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


// only compare the key of each map. the tuple is expected to diverge. first as bytestreams and only if they don't match
// get more details
CompareResult compareValues(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2,
                            const std::string& name) {
    if (h1.isMissing(name) && h2.isMissing(name)) {
        return CompareResult::Match;
    }

    if (h1.isMissing(name)) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                           << " is set to missing in first FDB but is not missing in second FDB" << std::endl;
        return CompareResult::OtherMismatch;
    }

    if (h2.isMissing(name)) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                           << " is set to missing in the second FDB but is not missing in first FDB" << std::endl;
        return CompareResult::OtherMismatch;
    }


    auto type1 = h1.type(name);
    auto type2 = h2.type(name);

    auto val1 = h1.get(name);
    auto val2 = h2.get(name);
    if (type1 != type2) {
        eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name << "Grib comparison Type mismatch: FDB1 Value "
                           << val1 << " CODES_TYPE " << type1 << "\nFDB2 value: " << val2 << " CODES_TYPE " << type2
                           << std::endl;
        return CompareResult::OtherMismatch;
    }


    if (holdsVector(val1)) {
        if (holdsVector(val2)) {
            auto l1 = vectorLength(val1);
            auto l2 = vectorLength(val2);
            if (l1 != l2) {
                eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << " Key" << name
                                   << " contains an array with different sizes "
                                   << " lengthRef= " << l1 << ", length2= " << l2 << std::endl;
                return CompareResult::OtherMismatch;
            }
        }
        else {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                               << "Grib comparison Type mismatch: FDB1 Value is a vector " << val1
                               << " \nFDB2 value is no vector: " << val2 << std::endl;
            return CompareResult::OtherMismatch;
        }
    }
    else {
        if (holdsVector(val2)) {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << name
                               << "Grib comparison Type mismatch: FDB1 Value is no vector " << val1
                               << " \nFDB2 value is vector: " << val2 << std::endl;
            return CompareResult::OtherMismatch;
        }
    }


    if (val2 != val1) {
        static const std::unordered_set<std::string> floatingValueFields{{"values", "packedValues", "codedValues"}};
        if (floatingValueFields.find(name) != floatingValueFields.end()) {
            // Return true and indicate that a check with a tolerance boundary needs to be made
            return CompareResult::DataSectionMismatch;
        }
        else {
            eckit::Log::info() << "[GRIB COMPARISON MISMATCH]" << " Key=" << name << ", FDB1 Value=" << val1
                               << ", FDB2 value:=" << val2 << std::endl;
            return CompareResult::OtherMismatch;
        }
    }

    return CompareResult::Match;
}


CompareResult compareKeyByKey(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2,
                              const std::unordered_set<std::string>& ignore_list,
                              const std::unordered_set<std::string>& match_list) {

    CompareResult res = CompareResult::Match;

    auto updateRes = [&](CompareResult singleResult) {
        switch (singleResult) {
            case CompareResult::OtherMismatch: {
                // If something mismatches, the overall result is always a mismatch
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

    if (match_list.size() != 0) {
        for (auto key : match_list) {
            updateRes(compareValues(h1, h2, key));
        }
    }
    else {
        for (const auto& keyIt : h1.keys(metkit::codes::KeyIteratorFlags::SkipComputed)) {
            auto name = keyIt.name();

            if (ignore_list.find(name) != ignore_list.end()) {
                continue;
            }

            updateRes(compareValues(h1, h2, name));
        }
    }
    return res;
}

}  // namespace compare::grib
