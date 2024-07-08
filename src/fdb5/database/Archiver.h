/*
 * (C) Copyright 1996- ECMWF.
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

#ifndef fdb5_Archiver_H
#define fdb5_Archiver_H

#include <time.h>
#include <utility>

#include "eckit/memory/NonCopyable.h"

#include "fdb5/database/DB.h"
#include "fdb5/config/Config.h"

namespace eckit   {
class DataHandle;
}

class MarsTask;

namespace fdb5 {

class Key;
class BaseArchiveVisitor;
class Schema;

//----------------------------------------------------------------------------------------------------------------------

class Archiver : public eckit::NonCopyable {

public: // methods

    Archiver(const Config& dbConfig = Config().expandConfig(), const ArchiveCallback& callback = CALLBACK_NOOP);

    virtual ~Archiver();

    void archive(const Key& key, BaseArchiveVisitor& visitor);
    void archive(const Key& key, const void* data, size_t len);

    /// Flushes all buffers and closes all data handles into a consistent DB state
    /// @note always safe to call
    void flush();

    friend std::ostream &operator<<(std::ostream &s, const Archiver &x) {
        x.print(s);
        return s;
    }

private: // methods

    void print(std::ostream &out) const;

    DB& database(const Key& key);

private: // members

    friend class BaseArchiveVisitor;

    typedef std::map< Key, std::pair<time_t, std::unique_ptr<DB> > > store_t;

    Config dbConfig_;

    store_t databases_;

    std::vector<Key> prev_;

    DB* current_;

    const ArchiveCallback& callback_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
