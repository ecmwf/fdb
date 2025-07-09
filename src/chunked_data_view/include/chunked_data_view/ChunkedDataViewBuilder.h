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


#include "chunked_data_view/AxisDefinition.h"
#include "chunked_data_view/ChunkedDataView.h"
#include "chunked_data_view/Fdb.h"

#include <memory>
#include <vector>

namespace chunked_data_view {
class ChunkedDataViewBuilder {
public:

    explicit ChunkedDataViewBuilder(std::unique_ptr<Fdb> fdb);
    /// Add data to the view defined by a mars request.
    /// Multiple parts can be added by repeated calls to addPart, e.g. adding surface and non surface fields.
    /// Every keyword with more than one value needs to be mapped to the output axes with an AxisDefinition.
    /// Multiple keywords can map to one axis.
    ChunkedDataViewBuilder& addPart(std::string marsRequestKeyValues, std::vector<AxisDefinition> axes,
                                    std::unique_ptr<Extractor> extractor);
    /// On which axis the multiple parts extend each other.
    /// For example the 3rd axis maps the
    /// TODO(kkratz): This is currently limiting and probably needs extension
    ChunkedDataViewBuilder& extendOnAxis(size_t index);
    std::unique_ptr<ChunkedDataView> build();

private:

    std::unique_ptr<Fdb> fdb_{};
    std::vector<std::tuple<std::string, std::vector<AxisDefinition>, std::unique_ptr<Extractor>>> parts_{};
    // TODO(kkratz): Should probably be an optional to facilitate the 1 request case
    size_t extensionAxisIndex_{};
};
}  // namespace chunked_data_view
