// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypeAbbreviation.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeAbbreviation : public Type {

public:  // methods

    TypeAbbreviation(const std::string& name, const std::string& type);

    ~TypeAbbreviation() override;

    std::string toKey(const std::string& value) const override;

    void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                   const CatalogueReader* cat) const override;

private:  // methods

    void print(std::ostream& out) const override;
    size_t count_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

