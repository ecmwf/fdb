/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AdoptVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_FDBFileHandle_h
#define fdb5_FDBFileHandle_h

#include "eckit/io/DataHandle.h"
#include "eckit/io/Buffer.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// This class was adapted from eckit::FileHandle
/// The differences are:
///   * it does not fsync() on flush, only on close()
///   * it fails on ENOSPC

class FDBFileHandle : public eckit::DataHandle {
public:

    // -- Contructors

    FDBFileHandle(const std::string&, size_t buffer);

    // -- Destructor

    ~FDBFileHandle();

    // --  Methods


    // -- Overridden methods

    // From eckit::DataHandle

    virtual eckit::Length openForRead();
    virtual void   openForWrite(const eckit::Length &);
    virtual void   openForAppend(const eckit::Length &);

    virtual long   read(void *, long);
    virtual long   write(const void *, long);
    virtual void   close();
    virtual void   flush();
    virtual void   print(std::ostream &) const;
    virtual eckit::Offset position();
    virtual std::string title() const;

protected: // members

    std::string      path_;

private: // members

    FILE            *file_;
    eckit::Buffer    buffer_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
