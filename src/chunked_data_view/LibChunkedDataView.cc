/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "chunked_data_view/LibChunkedDataView.h"

#include "chunked_data_view_version.h"

#include "eckit/runtime/Main.h"
#include "eckit/system/Library.h"

#include <algorithm>
#include <string>

namespace chunked_data_view {
void init_eckit_main() {
    const char* args[] = {"chunked_data_view", ""};
    eckit::Main::initialise(1, const_cast<char**>(args));
}

REGISTER_LIBRARY(LibChunkedDataView);

LibChunkedDataView::LibChunkedDataView() : Library("chunked_data_view") {}

const LibChunkedDataView& LibChunkedDataView::instance() {
    static LibChunkedDataView instance{};
    return instance;
}

std::string LibChunkedDataView::version() const {
    return std::string{version_string};
}

std::string LibChunkedDataView::gitsha1(unsigned int count) const {
    if (git_sha1.empty()) {
        return "not available";
    }

    return std::string{git_sha1.substr(0, std::min(count, 40u))};
};


}  // namespace chunked_data_view
