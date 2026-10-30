// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypeGrid.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeGrid : public Type {

public:  // methods

    TypeGrid(const std::string& name, const std::string& type);

    ~TypeGrid() override;

private:  // methods

    std::string toKey(const std::string& value) const override;

    void getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                   const CatalogueReader* cat) const override;

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
