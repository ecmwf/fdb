// SPDX-FileCopyrightText: 2025 European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "chunked_data_view/Fdb.h"

#include "chunked_data_view/ListIterator.h"

#include "fdb5/api/FDB.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include <filesystem>
#include <memory>
#include <optional>
#include <utility>

namespace chunked_data_view {

class FdbWrapper final : public FdbInterface {
public:

    explicit FdbWrapper(fdb5::FDB fdb) : fdb_(std::move(fdb)) {}

    std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) override {
        return std::unique_ptr<eckit::DataHandle>(fdb_.retrieve(request));
    };

    std::unique_ptr<chunked_data_view::ListIteratorInterface> inspect(
        const metkit::mars::MarsRequest& request) override {
        return makeListIterator(fdb_.inspect(request));
    }

private:

    fdb5::FDB fdb_{};
};

std::unique_ptr<FdbInterface> makeFdb(std::optional<std::filesystem::path> configPath) {
    if (configPath) {
        return std::make_unique<FdbWrapper>(fdb5::FDB(fdb5::Config::make(configPath->string())));
    }
    return std::make_unique<FdbWrapper>(fdb5::FDB());
}
}  // namespace chunked_data_view
