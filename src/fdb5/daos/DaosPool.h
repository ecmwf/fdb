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

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosSession;

class DaosPool {

public: // methods

    DaosPool(DaosPool&&) noexcept;
    ~DaosPool();

    // administrative
    void destroy(const int& force = 1);

    void open();
    void close();

    fdb5::DaosContainer& getContainer(uuid_t);
    fdb5::DaosContainer& getContainer(const std::string&);
    fdb5::DaosContainer& getContainer(uuid_t, const std::string&);

    fdb5::DaosContainer& createContainer(uuid_t);
    fdb5::DaosContainer& createContainer(const std::string&);
    fdb5::DaosContainer& createContainer(uuid_t, const std::string&);

    fdb5::DaosContainer& ensureContainer(uuid_t);
    fdb5::DaosContainer& ensureContainer(const std::string&);
    fdb5::DaosContainer& ensureContainer(uuid_t, const std::string&);

    void closeContainer(uuid_t);
    void closeContainer(const std::string&);
    void closeContainers();    
    void destroyContainers();

    std::vector<std::string> listContainers();

    const daos_handle_t& getOpenHandle();
    
    fdb5::DaosSession& getSession() const;
    std::string name() const;
    void uuid(uuid_t) const;
    std::string label() const;

private: // methods

    friend class DaosSession;

    DaosPool(fdb5::DaosSession&);
    DaosPool(fdb5::DaosSession&, uuid_t);
    DaosPool(fdb5::DaosSession&, const std::string&);
    DaosPool(fdb5::DaosSession&, uuid_t, const std::string&);

    void create(const uint64_t& scmSize, const uint64_t& nvmeSize);

    fdb5::DaosContainer& getContainer(uuid_t, bool);
    fdb5::DaosContainer& getContainer(const std::string&, bool);
    fdb5::DaosContainer& getContainer(uuid_t, const std::string&, bool);

    bool exists();

    std::deque<fdb5::DaosContainer>::iterator getCachedContainer(uuid_t);
    std::deque<fdb5::DaosContainer>::iterator getCachedContainer(const std::string&);

private: // members

    fdb5::DaosSession& session_;
    uuid_t uuid_;
    bool known_uuid_;
    std::string label_ = std::string();
    daos_handle_t poh_;
    bool open_;

    std::deque<fdb5::DaosContainer> cont_cache_;

};

class AutoPoolDestroy {

public: // methods

    AutoPoolDestroy(fdb5::DaosPool& pool) : pool_(pool) {}

    ~AutoPoolDestroy() noexcept(false) {

        bool fail = !eckit::Exception::throwing();

        try {
            pool_.destroy();
        }
        catch (std::exception& e) {
            eckit::Log::error() << "** " << e.what() << " Caught in " << Here() << std::endl;
            if (fail) {
                eckit::Log::error() << "** Exception is re-thrown" << std::endl;
                throw;
            }
            eckit::Log::error() << "** An exception is already in progress" << std::endl;
            eckit::Log::error() << "** Exception is ignored" << std::endl;
        }
        
    }

private: // members

    fdb5::DaosPool& pool_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5