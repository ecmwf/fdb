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
/// @date Jul 2022

#pragma once

#include <uuid/uuid.h>
#include <daos.h>

#include <string>
#include <deque>

// #include "fdb5/daos/DaosCluster.h"
#include "fdb5/daos/DaosContainer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosPool {

public: // methods

    ~DaosPool();

    // administrative
    void create();
    void destroy();

    void open();
    void close();

    fdb5::DaosContainer& declareContainer(uuid_t);
    fdb5::DaosContainer& declareContainer(const std::string&);
    fdb5::DaosContainer& declareContainer(uuid_t, const std::string&);

    fdb5::DaosContainer& createContainer(uuid_t);
    fdb5::DaosContainer& createContainer(const std::string&);
    fdb5::DaosContainer& createContainer(uuid_t, const std::string&);

    void destroyContainer(uuid_t);
    void destroyContainer(const std::string&);

    void closeContainer(uuid_t);
    void closeContainer(const std::string&);

    const daos_handle_t& getOpenHandle();

    std::string name() const;
    void uuid(uuid_t) const;
    std::string label() const;

private: // methods

    DaosPool();
    DaosPool(uuid_t);
    DaosPool(const std::string&);
    DaosPool(uuid_t, const std::string&);
    friend class DaosCluster;

    std::deque<fdb5::DaosContainer>::iterator getCachedContainer(uuid_t);
    std::deque<fdb5::DaosContainer>::iterator getCachedContainer(const std::string&);

private: // members

    uuid_t uuid_;
    bool known_uuid_;
    std::string label_ = std::string();
    daos_handle_t poh_;
    bool open_;

    std::deque<fdb5::DaosContainer> cont_cache_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5