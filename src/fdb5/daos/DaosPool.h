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

#include "fdb5/daos/DaosContainer.h"

#include "fdb5/fdb5_config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using ContainerCache = std::map<std::string, fdb5::DaosContainer>;

class DaosSession;

class DaosPool {

public: // methods

    DaosPool(DaosPool&&) noexcept;
    ~DaosPool();

    DaosPool& operator=(DaosPool&&) noexcept;

    void open();
    void close();

    fdb5::DaosContainer& getContainer(const std::string&);

    fdb5::DaosContainer& createContainer(const std::string&);

    fdb5::DaosContainer& ensureContainer(const std::string&);

    void destroyContainer(const std::string&);

    std::vector<std::string> listContainers();

    const daos_handle_t& getOpenHandle();
    
    std::string name() const;
    void uuid(uuid_t) const;
    std::string label() const;

private: // methods

    friend class DaosSession;

    DaosPool();
    DaosPool(uuid_t);
    DaosPool(const std::string&);
    DaosPool(uuid_t, const std::string&);

#ifdef fdb5_HAVE_DAOS_ADMIN
    void create(const uint64_t& scmSize, const uint64_t& nvmeSize);
#endif

    fdb5::DaosContainer& getContainer(const std::string&, bool);

    void closeContainers();    

    bool exists();

    ContainerCache::iterator getCachedContainer(const std::string&);

private: // members

    uuid_t uuid_;
    bool known_uuid_;
    std::string label_ = std::string();
    daos_handle_t poh_;
    bool open_;

    ContainerCache cont_cache_;

};

#ifdef fdb5_HAVE_DAOS_ADMIN
class AutoPoolDestroy {

public: // methods

    AutoPoolDestroy(fdb5::DaosPool& pool) : pool_(pool) {}

    ~AutoPoolDestroy() noexcept(false);

private: // members

    fdb5::DaosPool& pool_;

};
#endif

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
