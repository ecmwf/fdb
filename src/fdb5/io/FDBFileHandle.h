// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

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
