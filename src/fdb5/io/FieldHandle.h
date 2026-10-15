// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Emanuele Danovaro
/// @date March 2024

#pragma once

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"

#include "fdb5/api/helpers/ListIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FieldHandle : public eckit::DataHandle {
public:

    // -- Contructors

    FieldHandle(ListIterator& it);

    // -- Destructor

    ~FieldHandle() override;

    // -- Operators

    // -- Overridden methods

    // From DataHandle

    eckit::Length openForRead() override;

    long read(void*, long) override;
    void close() override;
    void rewind() override;
    void print(std::ostream&) const override;

    eckit::Offset position() override;
    eckit::Offset seek(const eckit::Offset&) override;
    bool canSeek() const override;

    bool compress(bool = false) override;

    eckit::Length size() override;
    eckit::Length estimate() override;

    // std::string title() const override;

    // // From Streamable

    // void encode(Stream&) const override;
    // const ReanimatorBase& reanimator() const override { return reanimator_; }

    // // -- Class methods

    // static const ClassSpec& classSpec() { return classSpec_; }

private:

    // -- Methods

    void openCurrent();
    void open();
    long read1(char*, long);

private:

    // -- Members

    std::vector<std::pair<eckit::Length, DataHandle*>> datahandles_;
    eckit::Length totalSize_;

    size_t currentIdx_;
    DataHandle* current_;
    bool currentMemoryHandle_;

    char* buffer_{nullptr};

    bool sorted_;
    bool seekable_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
