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

#include <daos.h>

#include <string>

#include "eckit/filesystem/URI.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosName;

class DaosContainer;

class DaosSession;

fdb5::DaosContainer& name_to_cont_ref(fdb5::DaosSession&, const fdb5::DaosName&);

class DaosObject {

public: // methods

    DaosObject(fdb5::DaosContainer&, const fdb5::DaosOID&);
    DaosObject(fdb5::DaosSession&, const fdb5::DaosName&);
    DaosObject(fdb5::DaosSession&, const eckit::URI&);
    DaosObject(DaosObject&&) noexcept;
    DaosObject(const DaosObject&) = default;
    // TODO: is that OK? Having a virtual destructor for base class and separate non-virutal destructor for derived classes. Should they override?
    virtual ~DaosObject() = default;

    virtual void destroy() = 0;

    virtual void open() = 0;
    virtual void close() = 0;

    const daos_handle_t& getOpenHandle();

    bool exists();
    virtual daos_size_t size() = 0;
    std::string name() const;
    fdb5::DaosOID OID() const;
    eckit::URI URI() const;
    fdb5::DaosContainer& getContainer() const;

private: // methods

    friend DaosContainer;

    DaosObject(fdb5::DaosContainer&, const fdb5::DaosOID&, bool verify);

    virtual void create() = 0;

protected: // members

    fdb5::DaosContainer& cont_;
    fdb5::DaosOID oid_;
    daos_handle_t oh_;
    bool open_;

};

class DaosArray : public DaosObject {

    using DaosObject::DaosObject;

    friend DaosContainer;

public: // methods

    ~DaosArray();

    void destroy() override;

    void open() override;
    void close() override;

    daos_size_t size() override;

    long write(const void*, const long&, const eckit::Offset&);
    long read(void*, const long&, const eckit::Offset&);

private: // methods

    void create() override;

};

class DaosKeyValue : public DaosObject {

    using DaosObject::DaosObject;

    friend DaosContainer;

public: // methods

    ~DaosKeyValue();

    void destroy() override;

    void open() override;
    void close() override;

    daos_size_t size() override { NOTIMP; };
    // doas_size_t length();
    // std::vector<std::string> keys();

    long put(const std::string& key, const void*, const long&);
    long get(const std::string& key, void*, const long&);

private: // methods

    void create() override;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5