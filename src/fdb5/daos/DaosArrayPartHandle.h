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
/// @date Oct 2023

#pragma once

#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosArray;

class DaosArrayName;

/// @note: because daos_array_read does not report actual amount of bytes read,
///   DaosArrayHandle::read must check the actual size and returns it if smaller
///   than the provided buffer, which reduces performance. DaosArrayPartHandle
///   circumvents that check for cases where the object size is known.
/// @note: see DaosArray::read
class DaosArrayPartHandle : public eckit::DataHandle {

public:  // methods

    DaosArrayPartHandle(const fdb5::DaosArrayName&, const eckit::Offset&, const eckit::Length&);

    ~DaosArrayPartHandle();

    virtual void print(std::ostream&) const override;

    // virtual void openForWrite(const eckit::Length&) override;
    // virtual void openForAppend(const eckit::Length&) override;
    virtual eckit::Length openForRead() override;

    // virtual long write(const void*, long) override;
    virtual long read(void*, long) override;
    virtual void close() override;
    virtual void flush() override;

    virtual eckit::Length size() override;
    virtual eckit::Length estimate() override;
    virtual eckit::Offset position() override;
    virtual eckit::Offset seek(const eckit::Offset&) override;
    virtual bool canSeek() const override;
    // virtual void skip(const eckit::Length&) override;

    // virtual void rewind() override;
    // virtual void restartReadFrom(const Offset&) override;
    // virtual void restartWriteFrom(const Offset&) override;

    virtual std::string title() const override;

    // virtual void encode(Stream&) const override;
    // virtual const ReanimatorBase& reanimator() const override { return reanimator_; }

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
    eckit::Length len_;

    // static ClassSpec classSpec_;
    // static Reanimator<DataHandle> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5