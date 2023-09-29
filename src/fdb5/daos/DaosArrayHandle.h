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

#include "eckit/io/DataHandle.h"

#include "fdb5/daos/DaosName.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosSession;

class DaosArray;

class DaosArrayName;

class DaosArrayHandle : public eckit::DataHandle {

public: // methods

    DaosArrayHandle(const fdb5::DaosArrayName&);

    ~DaosArrayHandle();

    virtual void print(std::ostream&) const override;

    virtual void openForWrite(const eckit::Length&) override;
    virtual void openForAppend(const eckit::Length&) override;
    virtual eckit::Length openForRead() override;

    virtual long write(const void*, long) override;
    virtual long read(void*, long) override;
    virtual void close() override;
    virtual void flush() override;

    virtual eckit::Length size() override;
    virtual eckit::Length estimate() override;
    virtual eckit::Offset position() override;
    virtual eckit::Offset seek(const eckit::Offset&) override;
    virtual bool canSeek() const override;
    virtual void skip(const eckit::Length&) override;

//     //virtual void rewind() override;
//     //virtual void restartReadFrom(const Offset&) override;
//     //virtual void restartWriteFrom(const Offset&) override;

    virtual std::string title() const override;

//     //virtual void encode(Stream&) const override;
//     //virtual const ReanimatorBase& reanimator() const override { return reanimator_; }

//     //static const ClassSpec& classSpec() { return classSpec_; }

private: // members

    // mutable because title() calls DaosArrayName::asString which may update (generate) OID
    mutable fdb5::DaosArrayName name_;
    std::unique_ptr<fdb5::DaosSession> session_;
    std::unique_ptr<fdb5::DaosArray> arr_;
    bool open_;
    eckit::Offset offset_;
    std::string mode_;

//     //static ClassSpec classSpec_;
//     //static Reanimator<DataHandle> reanimator_;

};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5