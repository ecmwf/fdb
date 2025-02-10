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

#include <optional>
#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosArray;

class DaosArrayName;

class DaosArrayHandle : public eckit::DataHandle {

public:  // methods

    DaosArrayHandle(const fdb5::DaosArrayName&);

    ~DaosArrayHandle();

    void print(std::ostream&) const override;

    void openForWrite(const eckit::Length&) override;
    void openForAppend(const eckit::Length&) override { NOTIMP; };
    eckit::Length openForRead() override;

    long write(const void*, long) override;
    long read(void*, long) override;
    void close() override;
    void flush() override;

    eckit::Length size() override;
    eckit::Length estimate() override;
    eckit::Offset position() override;
    eckit::Offset seek(const eckit::Offset&) override;
    bool canSeek() const override;
    // void skip(const eckit::Length&) override;

    // void rewind() override;
    // void restartReadFrom(const Offset&) override;
    // void restartWriteFrom(const Offset&) override;

    std::string title() const override;

    // void encode(Stream&) const override;
    // const ReanimatorBase& reanimator() const override { return reanimator_; }

    // static const ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    fdb5::DaosSession& session();

private:  // members

    // mutable because title() calls DaosArrayName::asString which may update (generate) OID
    mutable fdb5::DaosArrayName name_;
    std::optional<fdb5::DaosSession> session_;
    std::optional<fdb5::DaosArray> arr_;
    bool open_;
    eckit::Offset offset_;

    // static ClassSpec classSpec_;
    // static Reanimator<DataHandle> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5