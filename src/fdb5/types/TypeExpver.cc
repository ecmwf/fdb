// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <iomanip>
#include <sstream>

#include "eckit/utils/StringTools.h"
#include "eckit/utils/Translator.h"

#include "eckit/types/Date.h"

#include "fdb5/types/TypeExpver.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeExpver::TypeExpver(const std::string& name, const std::string& type) : Type(name, type) {}

TypeExpver::~TypeExpver() {}


std::string TypeExpver::tidy(const std::string& value) const {

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << eckit::StringTools::trim(value);
    return oss.str();
}


void TypeExpver::print(std::ostream& out) const {
    out << "TypeExpver[name=" << name_ << "]";
}

static TypeBuilder<TypeExpver> type("Expver");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
