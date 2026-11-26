// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Metin Cakircali
/// @date   Aug 2026

#pragma once

#include "eckit/log/Log.h"

#include <exception>
#include <ostream>
#include <utility>

namespace fdb5 {

// Runs `op`; on failure logs via Log::error and records the first exception into `first`.
// Intended for destructor-safe cleanup paths where all steps must be attempted.
template <typename Op>
void best_effort(std::exception_ptr& first, const char* context, Op&& op) {
    try {
        std::forward<Op>(op)();
    }
    catch (...) {
        if (!first) {
            first = std::current_exception();
        }
        try {
            throw;
        }
        catch (const std::exception& e) {
            eckit::Log::error() << context << ": " << e.what() << std::endl;
        }
        catch (...) {
            eckit::Log::error() << context << ": unknown exception" << std::endl;
        }
    }
}

// Runs `op` on every element of `range`, also when it fails for some of them, and rethrows the first failure.
template <typename Range, typename Op>
void for_each_best_effort(Range& range, const char* context, Op&& op) {
    std::exception_ptr first;
    for (auto& element : range) {
        best_effort(first, context, [&] { op(element); });
    }
    if (first) {
        std::rethrow_exception(first);
    }
}

}  // namespace fdb5
