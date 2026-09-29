// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
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
