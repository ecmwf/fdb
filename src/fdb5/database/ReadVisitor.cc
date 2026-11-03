// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

void ReadVisitor::values(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         const TypesRegistry& registry, eckit::StringList& values) {
    const auto& type = registry.lookupType(keyword);
    eckit::StringList list;
    type.getValues(request, keyword, list, catalogue_);

    std::optional<std::reference_wrapper<const eckit::DenseSet<std::string>>> filter;
    if (catalogue_) {
        filter = catalogue_->axis(type.alias());
    }

    for (const auto& value : list) {
        const std::string v = type.toKey(value);
        if (!filter || filter->get().find(v) != filter->get().end()) {
            values.push_back(value);
        }
    }
}
}  // namespace fdb5
