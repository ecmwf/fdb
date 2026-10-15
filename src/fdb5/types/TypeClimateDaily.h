// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypeClimateDaily.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeClimateDaily : public Type {

public:  // methods

    TypeClimateDaily(const std::string& name, const std::string& type);

    ~TypeClimateDaily() override;

    std::string toKey(const std::string& value) const override;

    void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                   const CatalogueReader* cat) const override;

    virtual bool match(const std::string&, const std::string& value1, const std::string& value2) const override;

private:  // methods

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

