/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Oct 2022

#pragma once

#include <daos.h>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosOID {

public: // methods

    DaosOID(const uint64_t&, const uint64_t&);
    DaosOID(const std::string&);

    // DaosOID(const DaosOID&);
    // DaosOID(DaosOID&&);
    // DaosOID& operator=(DaosOID);

    std::string asString() const;
    daos_obj_id_t asDaosObjIdT() const;

private: // methods

    DaosOID() = default;

    // TODO: is this the right approach? having a private default constructor for DaosOID and friending it here
    // so that oid_ in DaosName/DaosObject can be default initialised for constructors which do not initialise it explicitly
    friend class DaosName;
    friend class DaosObject;
    // TODO: this seems dangerous, as DaosOID can now be default initialised (i.e. invalid OID instances) from both of these classes

private: // members

    uint64_t hi_;
    uint64_t lo_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5