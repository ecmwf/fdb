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
/// @date   Mar 2018

#ifndef fdb5_api_FDB_H
#define fdb5_api_FDB_H

#include "fdb5/config/Config.h"

#include "eckit/utils/Regex.h"

#include <memory>

class MarsRequest;


namespace fdb5 {

class FDBBase;
class Key;

//----------------------------------------------------------------------------------------------------------------------

/// A handle to a general FDB

class FDB {

public: // methods

    FDB(const Config& config=Config());
    ~FDB();

    FDB(const FDB&) = delete;
    FDB(FDB&&) = default;

    FDB& operator=(const FDB&) = delete;
    FDB& operator=(FDB&&) = default;

    void archive(const Key& key, const void* data, size_t length);
    eckit::DataHandle* retrieve(const MarsRequest& request);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    bool matches(const Key& key);

private: // members

    std::map<std::string, eckit::Regex> select_;

    std::unique_ptr<FDBBase> internal_;

    bool dirty_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_api_FDB_H
