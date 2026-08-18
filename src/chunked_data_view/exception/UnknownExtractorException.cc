// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "chunked_data_view/exception/UnknownExtractorException.h"

namespace chunked_data_view {
UnknownExtractorException::UnknownExtractorException(const std::string& w) : Exception(w) {}

UnknownExtractorException::UnknownExtractorException(const std::string& w, const eckit::CodeLocation& l) :
    Exception(w, l) {}

}  // namespace chunked_data_view
