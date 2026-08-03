/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#pragma once

#include "eckit/exception/Exceptions.h"

namespace chunked_data_view {

/// Thrown by GribExtractor when field retrieval fails (e.g. the FDB iterator returns no
/// results for a request, or a field's value count does not match the expected DataLayout).
class GribExtractorException : public eckit::Exception {

public:

    GribExtractorException(const std::string&);
    GribExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
