/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "chunked_data_view/Fdb.h"

#include <fdb5/api/FDB.h>
#include <fdb5/config/Config.h>
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Key.h"

namespace chunked_data_view {

class FdbWrapper final : public Fdb {
public:

    explicit FdbWrapper(fdb5::FDB fdb) : fdb_(std::move(fdb)) {}

    std::unique_ptr<eckit::DataHandle> retrieve(const metkit::mars::MarsRequest& request) override {
        return std::unique_ptr<eckit::DataHandle>(fdb_.retrieve(request));
    };

    fdb5::ListIterator inspect(const metkit::mars::MarsRequest& request) override { return fdb_.inspect(request); }

private:

    fdb5::FDB fdb_{};
};

std::unique_ptr<Fdb> makeFdb(std::optional<std::filesystem::path> configPath) {
    if (configPath) {
        return std::make_unique<FdbWrapper>(fdb5::FDB(fdb5::Config::make(configPath->string())));
    }
    return std::make_unique<FdbWrapper>(fdb5::FDB());
}
}  // namespace chunked_data_view
