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
#include "eckit/io/Length.h"
#include "eckit/io/DataBlob.h"

#include "fdb5/DB.h"

namespace eckit   { class DataHandle; }
namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;
class Key;

//----------------------------------------------------------------------------------------------------------------------

class Archiver : public eckit::NonCopyable {

public: // methods

    Archiver();
    
    ~Archiver();

    /// Archives the data provided in the DataBlob, which contains an associated MetaData that is used for indexing
    /// @param source  data blob to read from
    ///
    void archive(eckit::DataBlobPtr source);

    /// Archives the data selected by the MarsRequest from the provided DataHandle
    /// @param source  data handle to read from
    ///
    void archive(const FdbTask& task, eckit::DataHandle& source);

    /// Archives the data in the buffer and described by the fdb::Key
    /// @param key metadata identifying the data
    /// @param data buffer
    /// @param length buffer length
    ///
    void archive(const Key& key, const void* data, eckit::Length length);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

private: // methods

    DB& session(const Key& key);

private: // members

    typedef std::vector< eckit::SharedPtr<DB> > store_t;

    store_t sessions_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif
