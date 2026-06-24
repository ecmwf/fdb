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

/// Thrown when an unrecognised ExtractorType is supplied to the factory.
class UnknownExtractorException : public eckit::Exception {

public:

    UnknownExtractorException(const std::string&);
    UnknownExtractorException(const std::string&, const eckit::CodeLocation&);
};
}  // namespace chunked_data_view
