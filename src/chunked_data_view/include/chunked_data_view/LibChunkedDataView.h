// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
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
