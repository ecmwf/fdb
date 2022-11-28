/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>
#include <iomanip>

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"

#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosOID::DaosOID(const uint64_t& hi, const uint64_t& lo) : hi_(hi), lo_(lo) {}

DaosOID::DaosOID(const std::string& s) {

    ASSERT(s.length() == 32);
    ASSERT(std::all_of(s.begin(), s.end(), ::isxdigit));

    hi_ = std::stoull(s.substr(0, 16), nullptr, 16);
    lo_ = std::stoull(s.substr(16, 16), nullptr, 16);

}

// DaosOID::DaosOID(const DaosOID& other) : hi_(other.hi_), lo_(other.lo_) {}

// DaosOID::DaosOID(DaosOID&& other) {

//     std::swap(hi_, other.hi_);
//     std::swap(lo_, other.lo_);

// }

// DaosOID& DaosOID::operator=(DaosOID rhs) {

//     std::swap(*this, rhs);
//     return *this;

// }

std::string DaosOID::asString() const {

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << hi_;
    os << std::setw(16) << std::setfill('0') << std::hex << lo_;
    return os.str();

}

daos_obj_id_t DaosOID::asDaosObjIdT() const {

    return daos_obj_id_t{hi_, lo_};

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5