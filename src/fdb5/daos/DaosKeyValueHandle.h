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
/// @date Feb 2023

#pragma once

#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosKeyValue;

class DaosKeyValueName;

class DaosKeyValueHandle : public eckit::DataHandle {

public: // methods

    DaosKeyValueHandle(const fdb5::DaosKeyValueName&, const std::string& key);

    ~DaosKeyValueHandle();

    virtual void print(std::ostream&) const override;

    virtual void openForWrite(const eckit::Length&) override;
    virtual eckit::Length openForRead() override;

    virtual long write(const void*, long) override;
    virtual long read(void*, long) override;
    virtual void close() override;
    virtual void flush() override;

    virtual eckit::Length size() override;
    virtual eckit::Length estimate() override;
    virtual eckit::Offset position() override;
    virtual bool canSeek() const override;

    virtual std::string title() const override;

private: // methods

    fdb5::DaosSession& session();

private: // members

    mutable fdb5::DaosKeyValueName name_;
    std::string key_;
    std::optional<fdb5::DaosSession> session_;
    std::optional<fdb5::DaosKeyValue> kv_;
    bool open_;
    eckit::Offset offset_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5