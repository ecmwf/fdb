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

#include "chunked_data_view/ListIterator.h"

#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"

#include <filesystem>
#include <memory>
#include <optional>

namespace chunked_data_view {

/// Abstract interface over an FDB instance, allowing the real FDB to be swapped out for
/// a mock in tests.
///
/// Two operations are needed: retrieving the raw field bytes for a single field (used to
/// probe the DataLayout), and listing all matching fields with their keys and data handles
/// (used when filling a chunk).
class FdbInterface {
public:

    virtual ~FdbInterface() = default;

    /// Returns a DataHandle positioned at the start of the field data for @p request.
    /// Used by Extractor::layout() to read the DataLayout of a single representative field.
    virtual std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) = 0;

    /// Returns an iterator over all fields matching @p request, yielding (key, data handle) pairs.
    virtual std::unique_ptr<ListIteratorInterface> inspect(const metkit::mars::MarsRequest& request) = 0;
};

/// Creates a real FDB instance, optionally configured from @p configPath.
std::unique_ptr<FdbInterface> makeFdb(std::optional<std::filesystem::path> configPath = std::nullopt);

};  // namespace chunked_data_view
