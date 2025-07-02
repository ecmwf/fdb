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

#include <filesystem>
#include <memory>
#include <optional>
#include "eckit/filesystem/URI.h"
#include "fdb5/api/helpers/ListIterator.h"

namespace eckit {
class DataHandle;
};

namespace metkit::mars {
class MarsRequest;
}

namespace chunked_data_view {

struct KeyDatahandlePair {
    fdb5::Key key;
    eckit::URI uri;
    std::unique_ptr<eckit::DataHandle> data_handle;
};

class Fdb {
public:

    virtual ~Fdb()                                                                                = default;
    virtual std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) = 0;
    virtual fdb5::ListIterator inspect(const metkit::mars::MarsRequest& request)                  = 0;
};

std::unique_ptr<Fdb> makeFdb(std::optional<std::filesystem::path> configPath = std::nullopt);

};  // namespace chunked_data_view
