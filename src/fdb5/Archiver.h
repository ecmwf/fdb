/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Archiver.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Archiver_H
#define fdb_Archiver_H

#include "eckit/memory/NonCopyable.h"

namespace eckit   { class DataHandle; }
namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;

//----------------------------------------------------------------------------------------------------------------------

class Archiver : public eckit::NonCopyable {

public: // methods

    Archiver(const FdbTask& task);
    
    ~Archiver();

    /// Archives the data selected by the MarsRequest from the provided DataHandle
    /// @param source  data handle to read from

    void archive(eckit::DataHandle& source);

private: // members

    const FdbTask& task_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
