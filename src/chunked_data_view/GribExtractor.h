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

#include "fdb5/api/FDB.h"
#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace chunked_data_view {
class GribExtractor final : public Extractor {
public:

    DataLayout layout(eckit::DataHandle& handle) const override;

    void writeInto(const metkit::mars::MarsRequest& request, std::unique_ptr<ListIteratorInterface> list_iterator,
                   const std::vector<Axis>& axes, const DataLayout& layout, float* ptr, size_t len,
                   size_t expected_msg_count) const override;
    void writeInto2(eckit::DataHandle& dataHandle, float* datumPtr, size_t datumLen) const override;
};
}  // namespace chunked_data_view
