/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "fdb5/database/ReadVisitor.h"

namespace fdb5 {

void ReadVisitor::values(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         const TypesRegistry& registry, eckit::StringList& values) {
    const auto& type = registry.lookupType(keyword);
    eckit::StringList list;
    type.getValues(request, keyword, list, wind_, catalogue_);

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
