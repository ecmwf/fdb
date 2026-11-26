// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <ostream>
#include <string>

#include "fdb5/types/Type.h"

namespace fdb5 {


class TypeYearMonth : public Type {

public:  // methods

    TypeYearMonth(const std::string& name, const std::string& type);

    std::string toKey(const std::string& value) const override;

    void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                   const CatalogueReader* cat) const override;

private:  // methods

    void print(std::ostream& out) const override;
};


}  // namespace fdb5
