/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_FDBFileHandle_h
#define fdb5_FDBFileHandle_h

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// This class was adapted from eckit::FileHandle
/// The differences are:
///   * it does not fsync() on flush, only on close()
///   * it fails on ENOSPC
///   * this class can only be used in Append mode
///   * this is not thread-safe neither multi-process safe

class FDBFileHandle : public eckit::DataHandle {
public:  // methods

    FDBFileHandle(const std::string&, size_t buffer);

    ~FDBFileHandle();

    eckit::Length openForRead() override;
    void openForWrite(const eckit::Length&) override;
    void openForAppend(const eckit::Length&) override;

    long read(void*, long) override;
    long write(const void*, long) override;
    void close() override;
    void flush() override;
    void print(std::ostream&) const override;
    eckit::Offset position() override;
    std::string title() const override;
    bool canSeek() const override { return false; }

protected:  // members

    std::string path_;

private:  // members

    FILE* file_;
    eckit::Buffer buffer_;
    off_t pos_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif
