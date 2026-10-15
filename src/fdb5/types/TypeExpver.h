// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypeExpver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypeExpver : public Type {

public:  // methods

    TypeExpver(const std::string& name, const std::string& type);

    ~TypeExpver() override;

    std::string tidy(const std::string& value) const override;

private:  // methods

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

