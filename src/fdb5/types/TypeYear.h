// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypeYear.h
/// @author Emanuele Danovaro
/// @date   January 2025

#pragma once

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeYear : public Type {

public:  // methods

    TypeYear(const std::string& name, const std::string& type, const std::string& alias = "year");

    ~TypeYear() override;

    std::string toKey(const std::string& value) const override;

    void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                   const CatalogueReader* cat) const override;

private:  // methods

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

