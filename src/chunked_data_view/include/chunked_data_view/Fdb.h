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

#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "metkit/mars/MarsRequest.h"

#include <filesystem>
#include <memory>
#include <optional>

namespace chunked_data_view {

class FdbInterface {
public:

    virtual ~FdbInterface() = default;
    virtual std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) = 0;
    virtual std::unique_ptr<ListIteratorInterface> inspect(const metkit::mars::MarsRequest& request) = 0;
};

std::unique_ptr<FdbInterface> makeFdb(std::optional<std::filesystem::path> configPath = std::nullopt);

};  // namespace chunked_data_view
