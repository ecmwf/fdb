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
///
/// @date Sept 2022

#pragma once

//#include "eckit/filesystem/URI.h"
//#include "fdb5/daos/DaosObject.h"

//#include <memory>

namespace fdb5 {

//class DaosObject;

class DaosName {

public:

    //DaosName(std::string title);
    //DaosName(std::string pool, std::string cont);
    //DaosName(std::string pool, std::string cont, std::string oid);
    //DaosName(eckit::URI);
    //~DaosName();

    //void destroy();
    //bool exists();
    //daos_size_t size();

    //std::string name();
    //eckit::URI URI();

    //eckit::DataHandle* dataHandle(bool overwrite = false) const;

private:

    //std::unique_ptr<fdb5::DaosObject> private_object;

};

}  // namespace fdb5