/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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

}  // namespace fdb5
