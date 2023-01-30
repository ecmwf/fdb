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
/// @date Sept 2022

#pragma once

#include <daos.h>

#include <string>

#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosSession;
class DaosPool;
class DaosContainer;

class DaosName {

public: // methods

    DaosName(const std::string& pool, const std::string& cont, const fdb5::DaosOID& oid);
    DaosName(const std::string& name);
    DaosName(const eckit::URI&);
    DaosName(const fdb5::DaosObject&);

    //void create();
    //void destroy();
    daos_size_t size();
    bool exists();
    // owner
    // empty

    std::string asString() const;
    eckit::URI URI() const;
    std::string poolName() const;
    std::string contName() const;
    fdb5::DaosOID OID() const;

    /// @todo: think if this overwrite parameter makes sense in DAOS
    eckit::DataHandle* dataHandle(bool overwrite = false) const;

private: // members

    std::string pool_;
    std::string cont_;
    fdb5::DaosOID oid_;
    std::unique_ptr<fdb5::DaosObject> obj_ = nullptr;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5