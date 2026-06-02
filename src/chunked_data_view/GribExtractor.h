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

#include "chunked_data_view/Axis.h"
#include "chunked_data_view/DataLayout.h"
#include "chunked_data_view/Extractor.h"
#include "chunked_data_view/ListIterator.h"

#include "chunked_data_view/ViewPart.h"
#include "fdb5/api/FDB.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace chunked_data_view {
class GribExtractor final : public Extractor {
public:

    GribExtractor(const std::shared_ptr<FdbInterface> fdb);

    DataLayout layout(eckit::DataHandle& handle) const override;

    size_t writeInto(std::unique_ptr<ListIteratorInterface> list_iterator, const std::vector<Axis>& axes,
                     const DataLayout& layout, float* ptr, size_t len, size_t extensionAxisIdx, size_t combinedExtSize,
                     size_t extensionOffset) const override;

    size_t extractInto(const ViewPart& part, const std::vector<std::size_t>& chunkIndex, float* ptr, size_t len,
                       size_t extensionAxisIdx, size_t combinedExtSize, size_t extensionOffset) const override;

private:

    std::shared_ptr<FdbInterface> _fdb;
};
}  // namespace chunked_data_view
