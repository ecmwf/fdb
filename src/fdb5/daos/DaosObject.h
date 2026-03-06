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

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosNameBase;

class DaosContainer;

class DaosSession;

class DaosObject {

    friend DaosContainer;

public:  // methods

    DaosObject(DaosObject&&) noexcept;
    DaosObject(const DaosObject&) = default;

    virtual ~DaosObject() = default;

    virtual daos_otype_t type() const = 0;

    virtual bool exists() = 0;
    virtual void destroy() = 0;
    virtual void open() = 0;
    virtual void close() = 0;
    virtual uint64_t size() = 0;

    std::string name() const;
    const fdb5::DaosOID& OID() const;
    eckit::URI URI() const;
    fdb5::DaosContainer& getContainer() const;

protected:  // methods

    DaosObject(fdb5::DaosContainer&, const fdb5::DaosOID&);

private:  // methods

    virtual void create() = 0;

protected:  // members

    fdb5::DaosContainer& cont_;
    fdb5::DaosOID oid_;
    daos_handle_t oh_;
    bool open_;
};

class DaosArray : public DaosObject {

    friend DaosContainer;

public:  // methods

    DaosArray(DaosArray&&) noexcept;

    DaosArray(fdb5::DaosContainer&, const fdb5::DaosOID&);
    DaosArray(fdb5::DaosSession&, const fdb5::DaosNameBase&);
    DaosArray(fdb5::DaosSession&, const eckit::URI&);

    ~DaosArray();

    daos_otype_t type() const override { return OID().otype(); }
    bool exists() override;
    void destroy() override;
    void open() override;
    void close() override;
    uint64_t size() override;

    uint64_t write(const void*, const uint64_t&, const eckit::Offset&);
    uint64_t read(void*, uint64_t, const eckit::Offset&);

private:  // methods

    DaosArray(fdb5::DaosContainer&, const fdb5::DaosOID&, bool verify);

    void create() override;
};

class DaosKeyValue : public DaosObject {

    friend DaosContainer;

public:  // methods

    DaosKeyValue(DaosKeyValue&&) noexcept;

    DaosKeyValue(fdb5::DaosContainer&, const fdb5::DaosOID&);
    DaosKeyValue(fdb5::DaosSession&, const fdb5::DaosNameBase&);
    DaosKeyValue(fdb5::DaosSession&, const eckit::URI&);

    ~DaosKeyValue();

    daos_otype_t type() const override { return DAOS_OT_KV_HASHED; }
    bool exists() override;
    void destroy() override;
    void open() override;
    void close() override;
    uint64_t size() override { NOTIMP; };

    uint64_t size(const std::string& key);
    bool has(const std::string& key);
    uint64_t put(const std::string& key, const void*, const uint64_t&);
    uint64_t get(const std::string& key, void*, const uint64_t&);
    void remove(const std::string& key);
    std::vector<std::string> keys();

    /// @note: expects empty vector
    eckit::MemoryStream getMemoryStream(std::vector<char>& v, const std::string& key,
                                        const std::string& kvTitle = "kv");

private:  // methods

    DaosKeyValue(fdb5::DaosContainer&, const fdb5::DaosOID&, bool verify);

    void create() override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
