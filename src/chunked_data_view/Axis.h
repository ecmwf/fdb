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

#include "fdb5/database/Key.h"

#include <cassert>
#include <cstddef>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

namespace chunked_data_view {

/// One MARS keyword together with the ordered list of its allowed values within an axis.
///
/// Multiple Parameters are composed into an Axis; the Axis index space is the Cartesian
/// product of all parameter value lists, enumerated in row-major order (last parameter
/// varies fastest).
class Parameter {

public:

    Parameter(std::tuple<const std::string, const std::vector<std::string>> tuple);
    Parameter(const std::string name, const std::vector<std::string> values);

    /// MARS keyword name (e.g. "date", "param").
    const std::string& name() const { return std::get<0>(_internal); }

    /// Ordered list of allowed string values for this keyword within the axis.
    const std::vector<std::string>& values() const { return std::get<1>(_internal); }

private:

    std::tuple<const std::string, const std::vector<std::string>> _internal;
};

/// Describes how one axis is divided into Zarr chunks.
///
/// Stores one extent entry per chunk; for uniform chunking all entries are equal.
/// The extensible flag marks axes using NoChunking: their single chunk grows when
/// additional parts are stitched onto the view along this axis.
class AxisChunks {

public:

    explicit AxisChunks(const std::vector<std::variant<size_t, std::tuple<size_t, size_t>>>& chunks, bool extensible) :
        extensible_(extensible) {
        for (const auto& element : chunks) {
            if (std::holds_alternative<size_t>(element)) {
                extensions_.emplace_back(std::get<size_t>(element));
            }
            else {
                auto [extent, amount] = std::get<std::tuple<size_t, size_t>>(element);
                for (size_t i = 0; i < amount; ++i) {
                    extensions_.emplace_back(extent);
                }
            }
        }
    }

    /// Convenience constructor: @p amount chunks each of size @p chunk_extension.
    AxisChunks(size_t chunk_extension, size_t amount, bool extensible) :
        AxisChunks(std::vector<std::variant<size_t, std::tuple<size_t, size_t>>>{std::tuple<size_t, size_t>{
                       chunk_extension, amount}},
                   extensible) {};

    /// Number of chunks along this axis.
    size_t size() const { return extensions_.size(); }

    /// True for NoChunking axes, whose combined extent is the sum of all parts' extents.
    bool isExtensible() const { return extensible_; }

    /// Per-chunk extents; each entry is the number of axis elements in that chunk.
    std::vector<size_t> extensions() const { return extensions_; }

    /// Extent of the first chunk; representative for all chunks when chunking is uniform.
    size_t representativeExtent() const { return extensions_[0]; }

private:

    std::vector<size_t> extensions_{};
    bool extensible_;
};


/// One dimension of the ChunkedDataView, formed from one or more MARS keywords.
///
/// The axis index space has size equal to the product of all constituent parameter value
/// counts, enumerated in row-major order (the last Parameter varies fastest).
/// index() maps a MARS key to the corresponding flat position within that space.
class Axis {
public:

    Axis(std::vector<chunked_data_view::Parameter> parameters);

    /// Total number of elements in this axis (product of all parameter value counts).
    size_t size() const { return size_; }

    /// Ordered list of MARS parameters that form this axis.
    const std::vector<chunked_data_view::Parameter>& parameters() const { return parameters_; }

    /// Returns the flat row-major index of the element identified by @p key.
    /// @throws eckit::Exception if @p key does not contain a value for every parameter,
    ///         or if a value is not present in the parameter's allowed-values list.
    size_t index(const fdb5::Key& key) const;

private:

    std::vector<chunked_data_view::Parameter> parameters_{};
    size_t size_{};
};

}  // namespace chunked_data_view
