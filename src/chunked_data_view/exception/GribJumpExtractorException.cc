// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/exception/GribJumpExtractorException.h"

namespace chunked_data_view {
GribJumpExtractorException::GribJumpExtractorException(const std::string& w) : Exception(w) {}

GribJumpExtractorException::GribJumpExtractorException(const std::string& w, const eckit::CodeLocation& l) :
    Exception(w, l) {}

}  // namespace chunked_data_view
