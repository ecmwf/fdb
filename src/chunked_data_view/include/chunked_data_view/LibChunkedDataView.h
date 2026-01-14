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

void init_eckit_main();

class LibChunkedDataView : public eckit::system::Library {
public:

    LibChunkedDataView();

    static const LibChunkedDataView& instance();

    std::string version() const override;

    std::string gitsha1(unsigned int count = 40) const override;
};

}  // namespace chunked_data_view
