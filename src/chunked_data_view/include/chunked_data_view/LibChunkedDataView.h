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

#include "eckit/system/Library.h"

#include <string>

namespace chunked_data_view {

/// Initialises the eckit main object when the library is used outside a full eckit application.
void init_eckit_main();

/// eckit library registration object for the chunked_data_view plugin.
/// Provides version and git-SHA metadata consumed by eckit's plugin infrastructure.
class LibChunkedDataView : public eckit::system::Library {
public:

    LibChunkedDataView();

    static const LibChunkedDataView& instance();

    std::string version() const override;

    std::string gitsha1(unsigned int count = 40) const override;
};

}  // namespace chunked_data_view
