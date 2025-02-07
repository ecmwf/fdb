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

#include <optional>

#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosKeyValue;

class DaosKeyValueName;

class DaosKeyValueHandle : public eckit::DataHandle {

public:  // methods
    DaosKeyValueHandle(const fdb5::DaosKeyValueName&, const std::string& key);

    ~DaosKeyValueHandle();

    void print(std::ostream&) const override;

    void openForWrite(const eckit::Length&) override;
    eckit::Length openForRead() override;

    long write(const void*, long) override;
    long read(void*, long) override;
    void close() override;
    void flush() override;

    eckit::Length size() override;
    eckit::Length estimate() override;
    eckit::Offset position() override;
    bool canSeek() const override;

    std::string title() const override;

private:  // methods
    fdb5::DaosSession& session();

private:  // members
    mutable fdb5::DaosKeyValueName name_;
    std::string key_;
    std::optional<fdb5::DaosSession> session_;
    std::optional<fdb5::DaosKeyValue> kv_;
    bool open_;
    eckit::Offset offset_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5