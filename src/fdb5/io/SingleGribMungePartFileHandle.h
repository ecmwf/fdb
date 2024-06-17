/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date May 2019

#ifndef fdb5_io_SingleGribMungePartFileHandle_h
#define fdb5_io_SingleGribMungePartFileHandle_h

#include <memory>

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/Key.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

// Given a key "substitute", reads the given GRIB message (offset/length _MUST_
// match to exactly one GRIB message). The GRIB message is then modified.

class SingleGribMungePartFileHandle : public eckit::DataHandle {
public:

// -- Contructors

    SingleGribMungePartFileHandle(const eckit::PathName&,
                                  const eckit::Offset&,
                                  const eckit::Length&,
                                  const CanonicalKey& substitute);
    SingleGribMungePartFileHandle(eckit::Stream&) { NOTIMP; }
    ~SingleGribMungePartFileHandle() override;

	// From DataHandle

    eckit::Length openForRead() override;
    void openForWrite(const eckit::Length&) override { NOTIMP; }
    void openForAppend(const eckit::Length&) override { NOTIMP; }

    long read(void*,long) override;
    long write(const void*,long) override { NOTIMP; }
    void close() override;
    void rewind() override { NOTIMP; }

    void print(std::ostream&) const override;
    bool merge(DataHandle*) override;
    bool compress(bool = false) override;
    eckit::Length size() override;
    eckit::Length estimate() override;

    void restartReadFrom(const eckit::Offset&) override { NOTIMP; }
    eckit::Offset seek(const eckit::Offset&) override { NOTIMP; }
    bool canSeek() const override { return false; }

    void toRemote(eckit::Stream&) const override { NOTIMP; }

    std::string title() const override;
    bool moveable() const override { return true; }
    eckit::DataHandle* clone() const override;

	// From Streamable

    void encode(eckit::Stream&) const override { NOTIMP; }
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

private: // members

    eckit::PathName name_;
    FILE*           file_;
    eckit::Offset   pos_;
    eckit::Offset   offset_;
    eckit::Length   length_;
    CanonicalKey             substitute_;
    std::unique_ptr<eckit::Buffer>  buffer_;

    // For Streamable

    static eckit::ClassSpec classSpec_;
    static eckit:: Reanimator<SingleGribMungePartFileHandle> reanimator_;
};


//-----------------------------------------------------------------------------

} // namespace eckit

#endif
