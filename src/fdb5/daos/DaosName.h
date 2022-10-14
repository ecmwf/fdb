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
//#include <memory>

#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosPool;
class DaosContainer;
class DaosObject;

class DaosName {

public: // methods

    DaosName(const std::string& pool, const std::string& cont, const std::string& oid);
    DaosName(const std::string& name);
    DaosName(const eckit::URI&);

    //void create();
    //void destroy();
    // TODO: return eckit::Length?
    daos_size_t size();
    //bool exists();

    std::string asString() const;
    eckit::URI URI() const;
    std::string poolName() const;
    std::string contName() const;
    std::string oid() const;

    // TODO: think if this overwrite parameter makes sense in DAOS
    eckit::DataHandle* dataHandle(bool overwrite = false) const;

private: // methods

    void createManagedObject();

private: // members

    std::string pool_;
    std::string cont_;
    std::string oid_;
    // TODO: cleaner way to initialise?
    std::unique_ptr<fdb5::DaosObject> obj_ = nullptr;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5